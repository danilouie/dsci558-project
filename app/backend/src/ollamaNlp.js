/**
 * Ollama JSON planner: single-shot NL → PlannerResponse → QuerySpec.
 * @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec
 * @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId
 * @typedef {import("../../shared/contracts.d.ts").SearchSortField} SearchSortField
 * @typedef {import("../../shared/contracts.d.ts").PlannerIntent} PlannerIntent
 * @typedef {import("../../shared/contracts.d.ts").PlannerFiltersPayload} PlannerFiltersPayload
 * @typedef {import("../../shared/contracts.d.ts").PlannerResponse} PlannerResponse
 */

import { ollamaLog, isOllamaLogEnabled, traceStep } from "./trace.js";

const OLLAMA_HOST = (process.env.OLLAMA_HOST || "http://127.0.0.1:11434").replace(/\/$/, "");
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "llama3";
const OLLAMA_TIMEOUT_MS = Math.max(5000, Number(process.env.OLLAMA_TIMEOUT_MS || 60000));

/**
 * Second-pass validation: strip filter values the planner invented but the user did not say.
 * Set `OLLAMA_PLANNER_GROUNDING=false` to disable (saves one Ollama round-trip).
 */
export const USE_PLANNER_GROUNDING = process.env.OLLAMA_PLANNER_GROUNDING !== "false";

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
  "price_drop",
  "pred_avg_quality"
]);

/** Single system prompt — board-game query planner (JSON only). */
export const BOARD_GAME_PLANNER_SYSTEM = `You are a query planner for a board game database.
Given a user message, output ONLY valid JSON with this exact schema:

{
  "intent": "filter" | "similarity" | "hybrid",
  "similarity_target": string | null,
  "filters": {
    "min_players": int | null,
    "max_players": int | null,
    "min_playtime": int | null,
    "max_playtime": int | null,
    "max_complexity": float | null,
    "min_complexity": float | null,
    "categories": [string] | null,
    "mechanisms": [string] | null,
    "min_avg_rating": float | null,
    "max_price": float | null,
    "year_from": int | null,
    "year_to": int | null,
    "max_min_age": int | null,
    "is_expansion": bool | null
  },
  "preset": string | null,
  "undervalued_only": bool | null,
  "overpriced_only": bool | null,
  "sort": string | null,
  "sort_direction": "asc" | "desc" | null,
  "result_limit": int,
  "explanation": string
}

Rules:
- If the user asks to find similar games: intent = "similarity"
- If the user asks to filter/search by attributes only: intent = "filter"
- If both are present (e.g. "games similar to Catan but for 2 players"): intent = "hybrid"
- similarity_target: anchor game title when intent is similarity or hybrid; otherwise null
- Only include filter fields that are explicitly or clearly implied; use null for omitted keys
- **Ridge value (NOT BGG rating):** In this app, "undervalued" / "good predicted quality" / "ridge undervalued" means high **pred_avg_quality** on games — NOT higher BGG geek rating. Do **not** set min_avg_rating just because the user said "undervalued"; only set min_avg_rating if they ask for a **BGG/average/geek rating** number or phrase (e.g. "rating at least 8").
- **Overpriced** means low pred_avg_quality (ridge), not low geek rating by itself — use overpriced_only or preset "overpriced", not min_avg_rating unless they asked for rating.
- preset: use when a named bundle fits: "best_under_budget", "value_for_price", "highly_rated_cheap", "overpriced", "undervalued", "high_want_low_own", "frequently_traded", "rating_per_dollar", "composite_demo", or null.
- undervalued_only: true when the user wants ridge-undervalued picks (high pred_avg_quality); pair with sort "pred_avg_quality" desc unless they ask to sort by something else (e.g. wants, wtt).
- overpriced_only: true for ridge-overpriced / low pred_avg_quality emphasis.
- "traded a lot" / "frequently traded" / high WTT on BGG → preset "frequently_traded" (sort wtt). You can combine with undervalued_only if they ask for both (e.g. undervalued games that trade often).
- sort / sort_direction: only if the user asks for a specific ordering; otherwise null (defaults apply downstream).
- result_limit: default 10 when unstated
- explanation: one short sentence of reasoning for logs only`;

/** Grounding agent: removes unstated numeric/topic constraints from the draft planner JSON. */
export const PLANNER_GROUNDING_SYSTEM = `You are a strict grounding editor for board-game query plans.

The user's message is the ONLY source of allowed constraints.

You receive the USER_MESSAGE and a DRAFT_PLAN JSON (same schema as below). Output ONLY valid JSON with this exact schema:

{
  "intent": "filter" | "similarity" | "hybrid",
  "similarity_target": string | null,
  "filters": {
    "min_players": int | null,
    "max_players": int | null,
    "min_playtime": int | null,
    "max_playtime": int | null,
    "max_complexity": float | null,
    "min_complexity": float | null,
    "categories": [string] | null,
    "mechanisms": [string] | null,
    "min_avg_rating": float | null,
    "max_price": float | null,
    "year_from": int | null,
    "year_to": int | null,
    "max_min_age": int | null,
    "is_expansion": bool | null
  },
  "preset": string | null,
  "undervalued_only": bool | null,
  "overpriced_only": bool | null,
  "sort": string | null,
  "sort_direction": "asc" | "desc" | null,
  "result_limit": int,
  "explanation": string
}

Rules:
- For every field inside "filters": set it to null (or empty array only if the key must be an array and nothing is grounded—use null for categories/mechanisms when nothing is grounded) if the USER_MESSAGE does not explicitly state it or clearly paraphrase it (same numbers, times, prices, player counts, category names, mechanism names, year ranges, complexity, expansion yes/no).
- Remove "helpful" invented numbers: e.g. do NOT keep max_price, min_avg_rating, min_players, etc. unless the user actually gave that bound.
- **Undervalued** in this product means ridge **pred_avg_quality** (high = undervalued), NOT BGG min_avg_rating. If the draft used min_avg_rating only because of the word "undervalued", remove min_avg_rating unless the user also asked for a concrete BGG/geek rating.
- Do NOT remove undervalued_only / preset "undervalued" when the user said undervalued (ridge sense). Do NOT remove preset "frequently_traded" when they said traded a lot / WTT.
- Phrases like "overpriced" WITHOUT numeric bounds do NOT justify max_price or min_avg_rating by themselves.
- Keep intent and similarity_target only if justified: e.g. similarity_target must match a game title the user referenced for similarity; otherwise null for filter-only messages.
- result_limit: keep a number only if the user asked for a count ("top 5", "10 games"); otherwise use 10.
- explanation: one short sentence listing what was removed or "unchanged", for logs.`;

/**
 * First balanced `{ ... }` in text (handles strings/escapes).
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
 * @param {{ temperature?: number, label?: string, format?: "json" }} [opts]
 */
export async function ollamaChat(messages, opts = {}) {
  const step = opts.label || "ollamaChat";
  if (isOllamaLogEnabled()) {
    ollamaLog(step, "prompt (messages + options)", {
      model: OLLAMA_MODEL,
      host: OLLAMA_HOST,
      temperature: opts.temperature ?? 0.1,
      format: opts.format,
      messages: messages.map((m) => ({ role: m.role, content: m.content }))
    });
  }
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), OLLAMA_TIMEOUT_MS);
  try {
    /** @type {Record<string, unknown>} */
    const body = {
      model: OLLAMA_MODEL,
      messages,
      stream: false,
      options: {
        temperature: opts.temperature ?? 0.1
      }
    };
    if (opts.format === "json") {
      body.format = "json";
    }
    const res = await fetch(`${OLLAMA_HOST}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
      body: JSON.stringify(body)
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
 * @param {unknown} v
 */
function numOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {unknown} raw
 * @returns {PlannerIntent}
 */
function coerceIntent(raw) {
  const s = String(raw || "").trim().toLowerCase();
  if (s === "filter" || s === "similarity" || s === "hybrid") return /** @type {PlannerIntent} */ (s);
  return "filter";
}

/**
 * Map planner nested filters → QuerySpec fragment (before sanitize).
 * @param {unknown} filtersRaw
 * @returns {Partial<QuerySpec>}
 */
export function plannerFiltersToQuerySpec(filtersRaw) {
  const f =
    filtersRaw && typeof filtersRaw === "object" && !Array.isArray(filtersRaw)
      ? /** @type {PlannerFiltersPayload} */ (filtersRaw)
      : /** @type {PlannerFiltersPayload} */ ({});

  /** @type {Partial<QuerySpec>} */
  const q = {};

  const mp = numOrNull(f.min_players);
  const xp = numOrNull(f.max_players);
  if (mp != null) q.filterMinPlayers = mp;
  if (xp != null) q.filterMaxPlayers = xp;

  const mi = numOrNull(f.min_playtime);
  const xa = numOrNull(f.max_playtime);
  if (mi != null) q.minPlaytime = mi;
  if (xa != null) q.maxTime = xa;

  const mic = numOrNull(f.min_complexity);
  const mac = numOrNull(f.max_complexity);
  if (mic != null) q.minComplexity = mic;
  if (mac != null) q.maxComplexity = mac;

  if (Array.isArray(f.categories) && f.categories.length) {
    q.categoryContains = f.categories.map((s) => String(s).toLowerCase().trim()).filter(Boolean);
  }
  if (Array.isArray(f.mechanisms) && f.mechanisms.length) {
    q.mechanismContains = f.mechanisms.map((s) => String(s).toLowerCase().trim()).filter(Boolean);
  }

  const mr = numOrNull(f.min_avg_rating);
  if (mr != null) q.minRating = mr;

  const mpz = numOrNull(f.max_price);
  if (mpz != null) q.maxPrice = mpz;

  const y0 = numOrNull(f.year_from);
  const y1 = numOrNull(f.year_to);
  if (y0 != null) q.minYear = Math.round(y0);
  if (y1 != null) q.maxYear = Math.round(y1);

  const age = numOrNull(f.max_min_age);
  if (age != null) q.maxMinAge = Math.round(age);

  if (f.is_expansion === true || f.is_expansion === false) {
    q.isExpansion = f.is_expansion;
  }

  return q;
}

/**
 * Parse and validate planner JSON from Ollama.
 * @param {Record<string, unknown>} o
 * @returns {PlannerResponse}
 */
export function parsePlannerResponse(o) {
  const intent = coerceIntent(o.intent);
  const st = o.similarity_target;
  const similarity_target =
    st != null && String(st).trim() !== "" ? String(st).trim().slice(0, 160) : null;

  const filters =
    o.filters && typeof o.filters === "object" && !Array.isArray(o.filters)
      ? /** @type {PlannerFiltersPayload} */ (o.filters)
      : {};

  let result_limit = numOrNull(o.result_limit);
  if (result_limit == null || result_limit < 1) result_limit = 10;
  result_limit = Math.min(200, Math.max(1, Math.round(result_limit)));

  const explanation =
    typeof o.explanation === "string" ? o.explanation.slice(0, 500) : "";

  /** @type {import("../../shared/contracts.d.ts").QueryPresetId | null} */
  let preset = null;
  const presetRaw = o.preset;
  if (typeof presetRaw === "string" && VALID_PRESETS.includes(/** @type {QueryPresetId} */ (presetRaw))) {
    preset = /** @type {import("../../shared/contracts.d.ts").QueryPresetId} */ (presetRaw);
  }

  const undervalued_only = o.undervalued_only === true;
  const overpriced_only = o.overpriced_only === true;

  /** @type {import("../../shared/contracts.d.ts").SearchSortField | null} */
  let sort = null;
  const sortRaw = o.sort;
  if (typeof sortRaw === "string" && VALID_SORT.includes(/** @type {SearchSortField} */ (sortRaw))) {
    sort = /** @type {import("../../shared/contracts.d.ts").SearchSortField} */ (sortRaw);
  }

  /** @type {"asc" | "desc" | null} */
  let sort_direction = null;
  const sd = o.sort_direction;
  if (sd === "asc" || sd === "desc") sort_direction = sd;

  return {
    intent,
    similarity_target,
    filters,
    result_limit,
    explanation,
    preset,
    undervalued_only,
    overpriced_only,
    sort,
    sort_direction
  };
}

/**
 * Map planner top-level fields (preset, ridge flags, sort) → QuerySpec fragment.
 * @param {import("../../shared/contracts.d.ts").PlannerResponse} plan
 * @returns {Partial<QuerySpec>}
 */
export function plannerResponseToQuerySpec(plan) {
  /** @type {Partial<QuerySpec>} */
  const q = {};

  if (plan.preset != null && VALID_PRESETS.includes(plan.preset)) {
    q.preset = plan.preset;
  }
  if (plan.undervalued_only === true) q.undervaluedOnly = true;
  if (plan.overpriced_only === true) q.overpricedOnly = true;
  if (plan.sort != null && VALID_SORT.includes(plan.sort)) {
    q.sort = plan.sort;
  }
  if (plan.sort_direction === "asc" || plan.sort_direction === "desc") {
    q.sortDirection = plan.sort_direction;
  }

  return q;
}

/**
 * @param {PlannerResponse} plan
 */
function summarizePlanForTrace(plan) {
  return {
    intent: plan.intent,
    hasTarget: Boolean(plan.similarity_target),
    filters: plan.filters,
    preset: plan.preset ?? null,
    undervalued_only: plan.undervalued_only === true,
    overpriced_only: plan.overpriced_only === true,
    sort: plan.sort ?? null,
    sort_direction: plan.sort_direction ?? null,
    result_limit: plan.result_limit
  };
}

/**
 * Second agent: strip hallucinated filters; on failure returns the draft unchanged.
 * @param {string} userMessage
 * @param {PlannerResponse} draft
 * @returns {Promise<PlannerResponse>}
 */
export async function groundPlannerAgainstUserMessage(userMessage, draft) {
  const msg = String(userMessage || "").trim();
  if (!msg) return draft;

  const payload = JSON.stringify({ user_message: msg, draft_plan: draft }, null, 2);
  const raw = await ollamaChat(
    [
      { role: "system", content: PLANNER_GROUNDING_SYSTEM },
      { role: "user", content: payload }
    ],
    { temperature: 0, format: "json", label: "planGroundingValidate" }
  );
  const o = parseJsonObject(raw);
  return parsePlannerResponse(o);
}

/**
 * Single Ollama round-trip → structured plan.
 * Optional second pass (default on) removes unstated filter fields.
 * @param {string} userMessage
 * @returns {Promise<PlannerResponse>}
 */
export async function planBoardGameQuery(userMessage) {
  const msg = String(userMessage || "").trim();
  const raw = await ollamaChat(
    [
      { role: "system", content: BOARD_GAME_PLANNER_SYSTEM },
      { role: "user", content: msg }
    ],
    { temperature: 0, format: "json", label: "planBoardGameQuery" }
  );
  const o = parseJsonObject(raw);
  /** @type {PlannerResponse} */
  let plan = parsePlannerResponse(o);
  ollamaLog("planBoardGameQuery", "parsed", { intent: plan.intent, hasTarget: Boolean(plan.similarity_target) });

  if (USE_PLANNER_GROUNDING && msg) {
    const before = summarizePlanForTrace(plan);
    try {
      plan = await groundPlannerAgainstUserMessage(msg, plan);
      traceStep("ollama", "planGroundingValidate merged", {
        before,
        after: summarizePlanForTrace(plan),
        groundingOn: true
      });
      ollamaLog("planGroundingValidate", "parsed", summarizePlanForTrace(plan));
    } catch (err) {
      traceStep("ollama", "planGroundingValidate failed; using draft plan", {
        message: /** @type {Error} */ (err).message
      });
    }
  }

  return plan;
}

/**
 * Coerce LLM / API partial spec into safe QuerySpec fields.
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
  if (raw.supportsPlayerCount != null) q.supportsPlayerCount = optNum(raw.supportsPlayerCount);
  if (raw.filterMinPlayers != null) q.filterMinPlayers = optNum(raw.filterMinPlayers);
  if (raw.filterMaxPlayers != null) q.filterMaxPlayers = optNum(raw.filterMaxPlayers);
  if (raw.maxTime != null) q.maxTime = optNum(raw.maxTime);
  if (raw.minPlaytime != null) q.minPlaytime = optNum(raw.minPlaytime);
  if (raw.maxPrice != null) q.maxPrice = optNum(raw.maxPrice);
  if (raw.minPrice != null) q.minPrice = optNum(raw.minPrice);
  if (raw.minRating != null) q.minRating = optNum(raw.minRating);
  if (raw.minComplexity != null) q.minComplexity = optNum(raw.minComplexity);
  if (raw.maxComplexity != null) q.maxComplexity = optNum(raw.maxComplexity);
  if (raw.minYear != null) q.minYear = optNum(raw.minYear);
  if (raw.maxYear != null) q.maxYear = optNum(raw.maxYear);
  if (raw.maxMinAge != null) q.maxMinAge = optNum(raw.maxMinAge);
  if (raw.minPredAvgQuality != null) q.minPredAvgQuality = optNum(raw.minPredAvgQuality);
  if (raw.maxPredAvgQuality != null) q.maxPredAvgQuality = optNum(raw.maxPredAvgQuality);
  if (raw.minWants != null) q.minWants = optNum(raw.minWants);
  if (raw.minOwns != null) q.minOwns = optNum(raw.minOwns);
  if (raw.priceWindowDays != null) q.priceWindowDays = optNum(raw.priceWindowDays);
  if (raw.minPriceDrop != null) q.minPriceDrop = optNum(raw.minPriceDrop);
  if (raw.limit != null) {
    const L = optNum(raw.limit);
    if (L != null) q.limit = Math.min(200, Math.max(1, Math.round(L)));
  }

  if (raw.undervaluedOnly === true) q.undervaluedOnly = true;
  if (raw.overpricedOnly === true) q.overpricedOnly = true;

  if (raw.usernameExcludesOwns != null && String(raw.usernameExcludesOwns).trim()) {
    q.usernameExcludesOwns = String(raw.usernameExcludesOwns).trim();
  }

  if (raw.isExpansion === true || raw.isExpansion === false) {
    q.isExpansion = raw.isExpansion;
  }

  if (Array.isArray(raw.categoryContains) && raw.categoryContains.length) {
    q.categoryContains = raw.categoryContains.map((s) => String(s).toLowerCase().trim()).slice(0, 40);
  }
  if (Array.isArray(raw.mechanismContains) && raw.mechanismContains.length) {
    q.mechanismContains = raw.mechanismContains.map((s) => String(s).toLowerCase().trim()).slice(0, 40);
  }

  if (Array.isArray(raw.bggIdAllowList) && raw.bggIdAllowList.length) {
    q.bggIdAllowList = raw.bggIdAllowList.map((x) => String(x)).slice(0, 500);
  }

  return q;
}
