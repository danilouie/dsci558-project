/**
 * Ollama JSON extraction for NL → QuerySpec and prompt routing.
 * @typedef {import("../../shared/contracts.d.ts").PromptKind} PromptKind
 * @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec
 * @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId
 * @typedef {import("../../shared/contracts.d.ts").SearchSortField} SearchSortField
 */

import { ollamaLog, isOllamaLogEnabled } from "./trace.js";

const OLLAMA_HOST = (process.env.OLLAMA_HOST || "http://127.0.0.1:11434").replace(/\/$/, "");
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "llama3";
const OLLAMA_TIMEOUT_MS = Math.max(5000, Number(process.env.OLLAMA_TIMEOUT_MS || 60000));

const VALID_PRESETS = /** @type {QueryPresetId[]} */ ([
  "best_under_budget",
  "value_for_price",
  "highly_rated_cheap",
  "overpriced",
  "undervalued",
  "high_want_low_own",
  "frequently_traded",
  "rating_per_dollar",
  "composite_demo"
]);

const VALID_SORT = /** @type {SearchSortField[]} */ ([
  "rating",
  "geek_rating",
  "rank_value_asc",
  "mean_price",
  "rating_per_dollar",
  "want_minus_own",
  "wtt_to_wants",
  "value_score",
  "wants",
  "wtt",
  "price_drop"
]);

/** Keys Step A may emit (subset of QuerySpec). */
export const FILTER_ACTIVE_KEYS = [
  "keyword",
  "preset",
  "players",
  "maxTime",
  "maxPrice",
  "minPrice",
  "minRating",
  "categoryContains",
  "mechanismContains",
  "minComplexity",
  "maxComplexity",
  "minWants",
  "minOwns",
  "undervaluedOnly",
  "overpricedOnly",
  "sort",
  "sortDirection",
  "usernameExcludesOwns",
  "priceWindowDays",
  "minPriceDrop"
];

/**
 * First balanced `{ ... }` in text (handles strings/escapes). Models often prepend
 * prose like "Here's the JSON:" before the object.
 * @param {string} raw
 * @returns {string | null}
 */
export function extractFirstJsonObject(raw) {
  const s = String(raw || "");
  const start = s.indexOf("{");
  if (start === -1) return null;
  let depth = 0;
  let inString = false;
  let escape = false;
  for (let i = start; i < s.length; i++) {
    const c = s[i];
    if (inString) {
      if (escape) {
        escape = false;
      } else if (c === "\\") {
        escape = true;
      } else if (c === '"') {
        inString = false;
      }
      continue;
    }
    if (c === '"') {
      inString = true;
      continue;
    }
    if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) return s.slice(start, i + 1);
    }
  }
  return null;
}

/**
 * Strip ```json fences and parse JSON object.
 * Falls back to extracting the first `{...}` when the model wraps output in prose.
 * @param {string} raw
 */
export function parseJsonObject(raw) {
  let s = String(raw || "").trim();
  s = s.replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/i, "").trim();
  /** @param {string} json */
  const asObject = (json) => {
    const obj = JSON.parse(json);
    if (obj === null || typeof obj !== "object" || Array.isArray(obj)) {
      throw new Error("Expected JSON object");
    }
    return /** @type {Record<string, unknown>} */ (obj);
  };
  try {
    return asObject(s);
  } catch {
    const extracted = extractFirstJsonObject(s);
    if (!extracted) {
      throw new Error(`No JSON object in model output: ${s.slice(0, 120)}`);
    }
    return asObject(extracted);
  }
}

/**
 * @param {{ role: string, content: string }[]} messages
 * @param {{ temperature?: number, label?: string }} [opts] - `label` names the NL pipeline step in logs
 */
export async function ollamaChat(messages, opts = {}) {
  const step = opts.label || "ollamaChat";
  if (isOllamaLogEnabled()) {
    ollamaLog(step, "prompt (messages + options)", {
      model: OLLAMA_MODEL,
      host: OLLAMA_HOST,
      temperature: opts.temperature ?? 0.1,
      messages: messages.map((m) => ({ role: m.role, content: m.content }))
    });
  }
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), OLLAMA_TIMEOUT_MS);
  try {
    const res = await fetch(`${OLLAMA_HOST}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify({
        model: OLLAMA_MODEL,
        messages,
        stream: false,
        options: {
          temperature: opts.temperature ?? 0.1
        }
      })
    });
    if (!res.ok) {
      const text = await res.text();
      if (isOllamaLogEnabled()) ollamaLog(step, "http_error", { status: res.status, body: text.slice(0, 500) });
      throw new Error(`Ollama HTTP ${res.status}: ${text.slice(0, 200)}`);
    }
    const data = await res.json();
    const content = data?.message?.content;
    if (typeof content !== "string") {
      if (isOllamaLogEnabled()) ollamaLog(step, "error_missing_content", { data });
      throw new Error("Ollama response missing message.content");
    }
    if (isOllamaLogEnabled()) ollamaLog(step, "response_raw", content);
    return content;
  } finally {
    clearTimeout(t);
  }
}

/**
 * @param {string} message
 * @returns {Promise<{ promptKind: PromptKind, similarToGame: string | null }>}
 */
export async function classifyPromptKind(message) {
  const sys = `You route board-game discovery chat prompts.
Return ONLY valid JSON (no markdown):
{"promptKind":"similar_to_game"|"filtering"|"both","similarToGame":string|null}

Definitions:
- similar_to_game: user wants games LIKE / SIMILAR TO / reminiscent of a named title only (e.g. "like Catan") with no extra budget/category/mechanism constraints.
- filtering: only constraints — budget, player count, play time, presets ("high want low own"), undervalued, categories, mechanisms, sort — no named reference game.
- both: combines a reference game AND any other constraint — price, players, time, category/mechanism (including quoted "strategy" category), presets, sorts.

similarToGame: short anchor title ONLY when promptKind is similar_to_game or both; otherwise null.
Examples:
- "games like Terraforming Mars" -> {"promptKind":"similar_to_game","similarToGame":"Terraforming Mars"}
- "under $40 for 4 players" -> {"promptKind":"filtering","similarToGame":null}
- "high want low own similar to Gloomhaven" -> {"promptKind":"both","similarToGame":"Gloomhaven"}
- "similar to Catan in the strategy category" -> {"promptKind":"both","similarToGame":"Catan"}
- "games like Wingspan in this \\"economic\\" category" -> {"promptKind":"both","similarToGame":"Wingspan"}
- "similar game like Catan in \\"wargame\\" category" -> {"promptKind":"both","similarToGame":"Catan"}`;

  const raw = await ollamaChat(
    [
      { role: "system", content: sys },
      { role: "user", content: String(message).trim() }
    ],
    { temperature: 0, label: "1.classifyPromptKind" }
  );
  const o = parseJsonObject(raw);
  const pk = o.promptKind;
  if (pk !== "similar_to_game" && pk !== "filtering" && pk !== "both") {
    throw new Error(`Invalid promptKind: ${pk}`);
  }
  const st = o.similarToGame;
  const similarToGame =
    st != null && String(st).trim() !== "" ? String(st).trim().slice(0, 120) : null;
  const out = { promptKind: /** @type {PromptKind} */ (pk), similarToGame };
  ollamaLog("1.classifyPromptKind", "parsed", out);
  return out;
}

/**
 * Pick at most one category shard slug for description FAISS, or null for all_games.
 * @param {string} message
 * @param {string[]} allowedSlugs - from GET /v1/category-slugs (exact folder names)
 * @returns {Promise<string | null>}
 */
export async function selectFaissCategorySlug(message, allowedSlugs) {
  const list = (allowedSlugs || []).filter(Boolean);
  if (!list.length) return null;

  const slugList = list.slice().sort().join(", ");
  const sys = `You route board-game similarity search to ONE optional per-category embedding index (shard), or none.

Return ONLY valid JSON (no markdown):
{"faissCategorySlug": string | null}

Allowed values for faissCategorySlug — EXACT strings (pick one verbatim or null):
${slugList}

Rules:
- Use null when the user wants generally similar games across the whole catalog with no category-scoped vector index.
  - Set faissCategorySlug ONLY when they clearly tie similarity to a category family that matches one slug.
  - If unsure or no slug fits, null.
- Don't include category slug if the user is not explicitly asking for a category-scoped similarity search.

Examples:
- "games similar to Everdell in fantasy" -> {"faissCategorySlug":"fantasy"}
- "cheap games like Azul" -> {"faissCategorySlug":null}`;

  const raw = await ollamaChat(
    [{ role: "system", content: sys }, { role: "user", content: String(message).trim() }],
    { temperature: 0, label: "5.selectFaissCategorySlug" }
  );
  const o = parseJsonObject(raw);
  const slug = o.faissCategorySlug;
  if (slug == null || String(slug).trim() === "") {
    ollamaLog("5.selectFaissCategorySlug", "parsed", { faissCategorySlug: null, allowedMatch: true });
    return null;
  }
  const s = String(slug).trim();
  const set = new Set(list);
  const out = set.has(s) ? s : null;
  ollamaLog("5.selectFaissCategorySlug", "parsed", { modelSlug: s, allowlistHit: out !== null, faissCategorySlug: out });
  return out;
}

/**
 * Step A — which filter dimensions apply.
 * @param {string} message
 */
export async function filterStepActiveKinds(message) {
  const keys = FILTER_ACTIVE_KEYS.join(", ");
  const sys = `You list which board-game search filters the user intends. Return ONLY valid JSON:
{"active": string[]}

Allowed filter keys (use exact strings): ${keys}

Include a key only if the user clearly implies it. Use "preset" when they match a preset-style intent (budget shoppers, undervalued, high want low own, trades, etc.).
ALWAYS include "maxPrice" when they cap budget: under/below/less than $X or X dollars.
ALWAYS include "minRating" when they require a rating floor.
IMPORTANT: If the user restricts by **board-game category** (e.g. "in the strategy category", "in this \\"economic\\" category", "category \\"fantasy\\""), you MUST include "categoryContains".
If they restrict by **mechanism** (e.g. "worker placement"), include "mechanismContains".
If the message ONLY says they want games **similar/like** a **named title** and states NO budget, players, time, category, mechanism, preset, sort, or rating, return **{"active":[]}"** (do not use "keyword" for the title — the similarity anchor is not a name search).
Empty array if nothing filter-specific.`;

  const raw = await ollamaChat(
    [
      { role: "system", content: sys },
      { role: "user", content: String(message).trim() }
    ],
    { temperature: 0, label: "2.filterStepActiveKinds" }
  );
  const o = parseJsonObject(raw);
  const active = o.active;
  if (!Array.isArray(active)) {
    throw new Error("filterStepActiveKinds: missing active array");
  }
  const set = new Set(FILTER_ACTIVE_KEYS);
  const out = active.map((x) => String(x)).filter((k) => set.has(k));
  ollamaLog("2.filterStepActiveKinds", "parsed", { active: out });
  return out;
}

/**
 * Step B — extract values for selected keys only.
 * @param {string} message
 * @param {string[]} active
 */
export async function filterStepExtractValues(message, active) {
  if (!active.length) {
    ollamaLog("3.filterStepExtractValues", "skip (no active keys from step 2)", {});
    return {};
  }

  const presetList = VALID_PRESETS.join(", ");
  const sortList = VALID_SORT.join(", ");

  const sys = `You extract structured filter values for a board-game database QuerySpec.
Return ONLY valid JSON with keys that appear in THIS list (omit keys with no value):
ACTIVE_KEYS: ${JSON.stringify(active)}

Rules:
- preset must be one of: ${presetList} or null
- sort must be one of: ${sortList} or null
- sortDirection: "asc" or "desc" when sort is set
- categoryContains / mechanismContains: arrays of short lowercase substrings that match BGG-style category/mechanism names (e.g. user says in \\"strategy\\" category -> ["strategy"]; "economic category" -> ["economic"])
- players, maxTime, maxPrice, minPrice, minRating, minWants, minOwns, minComplexity, maxComplexity: numbers only when stated
- undervaluedOnly, overpricedOnly: booleans
- keyword: short search substring for game name ONLY when user gives a direct title search (not the "similar to X" anchor — that is handled elsewhere)
- Use null or omit fields you cannot infer`;

  const raw = await ollamaChat(
    [
      { role: "system", content: sys },
      { role: "user", content: String(message).trim() }
    ],
    { temperature: 0, label: "3.filterStepExtractValues" }
  );
  const o = parseJsonObject(raw);
  /** @type {Partial<QuerySpec>} */
  const out = {};
  for (const k of active) {
    if (Object.prototype.hasOwnProperty.call(o, k)) {
      out[/** @type {keyof QuerySpec} */ (k)] = /** @type {any} */ (o[k]);
    }
  }
  ollamaLog("3.filterStepExtractValues", "parsed (subset by active keys)", out);
  return out;
}

/**
 * Step C — re-read the user message and output corrected filter fields (no regex; pure LLM).
 * Merged over the A+B result so missed keys are filled.
 * @param {string} message
 * @param {Partial<import("../../shared/contracts.d.ts").QuerySpec>} currentPartial
 * @returns {Promise<Partial<import("../../shared/contracts.d.ts").QuerySpec>>}
 */
export async function reconcileQuerySpecFromMessage(message, currentPartial) {
  const currentJson = JSON.stringify(currentPartial || {});
  const keys = FILTER_ACTIVE_KEYS.join(", ");
  const sys = `You complete a board-game search QuerySpec from the user's words only.

Return ONLY a JSON object (no markdown, no text before or after) whose keys are a **subset of**: ${keys}

The earlier extraction pass produced (may be incomplete or wrong; fix it if needed):
${currentJson}

Rules:
- Only include a key if the user clearly asked for that filter.
- maxPrice: dollars (e.g. "under 35" / "under $35" -> 35, "less than 50 dollars" -> 50).
- minPrice: if they say "over $X" for price floor.
- minRating: 1-10. "Geek rating", "BGG rating", "at least 7" -> set minRating.
- categoryContains / mechanismContains: lowercase substrings, arrays.
- players, maxTime, minComplexity, maxComplexity, preset, sort, undervaluedOnly, overpricedOnly: as usual.
- If the message has no structural filters, return exactly: {}.
- Do not invent constraints the user did not state.
- The earlier pass may have wrongly set "keyword" to a "similar to X" title, or a "preset" the user never asked for — return only what the user actually said, and use \`"preset": null\` in JSON to drop a spurious earlier preset when the user did not request that preset.`;

  const raw = await ollamaChat(
    [
      { role: "system", content: sys },
      { role: "user", content: String(message).trim() }
    ],
    { temperature: 0, label: "4.reconcileQuerySpec" }
  );
  const o = parseJsonObject(raw);
  const sanitized = sanitizeQuerySpec(/** @type {Partial<import("../../shared/contracts.d.ts").QuerySpec>} */ (o));
  ollamaLog("4.reconcileQuerySpec", "parsed (after sanitizeQuerySpec)", sanitized);
  return sanitized;
}

/**
 * Coerce LLM partial spec into safe QuerySpec fields.
 * @param {Partial<QuerySpec>} raw
 */
export function sanitizeQuerySpec(raw) {
  /** @type {Partial<QuerySpec>} */
  const q = {};

  if (Object.hasOwn(/** @type {object} */ (raw), "keyword")) {
    if (raw.keyword == null || !String(raw.keyword).trim()) {
      q.keyword = null;
    } else {
      q.keyword = String(raw.keyword).trim().slice(0, 200);
    }
  }
  if (Object.hasOwn(/** @type {object} */ (raw), "preset")) {
    if (raw.preset == null || (typeof raw.preset === "string" && !String(raw.preset).trim())) {
      q.preset = null;
    } else if (VALID_PRESETS.includes(/** @type {QueryPresetId} */ (raw.preset))) {
      q.preset = /** @type {QueryPresetId} */ (raw.preset);
    }
  }
  if (raw.sort != null && VALID_SORT.includes(/** @type {SearchSortField} */ (raw.sort))) {
    q.sort = /** @type {SearchSortField} */ (raw.sort);
  }
  if (raw.sortDirection === "asc" || raw.sortDirection === "desc") {
    q.sortDirection = raw.sortDirection;
  }

  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };

  const optNum = (v) => {
    const n = num(v);
    return n == null ? undefined : n;
  };

  if (raw.players != null) q.players = optNum(raw.players);
  if (raw.maxTime != null) q.maxTime = optNum(raw.maxTime);
  if (raw.maxPrice != null) q.maxPrice = optNum(raw.maxPrice);
  if (raw.minPrice != null) q.minPrice = optNum(raw.minPrice);
  if (raw.minRating != null) q.minRating = optNum(raw.minRating);
  if (raw.minComplexity != null) q.minComplexity = optNum(raw.minComplexity);
  if (raw.maxComplexity != null) q.maxComplexity = optNum(raw.maxComplexity);
  if (raw.minWants != null) q.minWants = optNum(raw.minWants);
  if (raw.minOwns != null) q.minOwns = optNum(raw.minOwns);
  if (raw.priceWindowDays != null) q.priceWindowDays = optNum(raw.priceWindowDays);
  if (raw.minPriceDrop != null) q.minPriceDrop = optNum(raw.minPriceDrop);

  if (raw.undervaluedOnly === true) q.undervaluedOnly = true;
  if (raw.overpricedOnly === true) q.overpricedOnly = true;

  if (raw.usernameExcludesOwns != null && String(raw.usernameExcludesOwns).trim()) {
    q.usernameExcludesOwns = String(raw.usernameExcludesOwns).trim();
  }

  if (Array.isArray(raw.categoryContains) && raw.categoryContains.length) {
    q.categoryContains = raw.categoryContains.map((s) => String(s).toLowerCase()).slice(0, 20);
  }
  if (Array.isArray(raw.mechanismContains) && raw.mechanismContains.length) {
    q.mechanismContains = raw.mechanismContains.map((s) => String(s).toLowerCase()).slice(0, 20);
  }

  return q;
}
