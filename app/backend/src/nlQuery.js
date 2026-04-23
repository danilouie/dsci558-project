/**
 * NL → QuerySpec: single Ollama planner + optional FAISS similarity.
 */
import {
  planBoardGameQuery,
  plannerFiltersToQuerySpec,
  plannerResponseToQuerySpec,
  sanitizeQuerySpec
} from "./ollamaNlp.js";
import { faissResolveName, faissSimilarByDescription } from "./faissClient.js";
import { mergeQuerySpec, messageToQuerySpec } from "./searchQuery.js";
import { traceStep } from "./trace.js";

/** @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec */
/** @typedef {import("../../shared/contracts.d.ts").NlParseMeta} NlParseMeta */
/** @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId */
/** @typedef {import("../../shared/contracts.d.ts").PlannerIntent} PlannerIntent */

/**
 * Default sort for ridge undervalued/overpriced when the planner did not set sort and
 * preset does not already imply ordering (e.g. frequently_traded → wtt).
 * @param {Partial<QuerySpec>} partial
 */
function applyPlannerRidgeDefaults(partial) {
  if (partial.sort != null) return;
  const preset = partial.preset ?? null;
  if (preset === "frequently_traded" || preset === "undervalued" || preset === "overpriced") {
    return;
  }
  if (partial.undervaluedOnly && !partial.overpricedOnly) {
    partial.sort = "pred_avg_quality";
    partial.sortDirection = partial.sortDirection || "desc";
  } else if (partial.overpricedOnly) {
    partial.sort = "pred_avg_quality";
    partial.sortDirection = partial.sortDirection || "asc";
  }
}

export const USE_OLLAMA_NL = (process.env.USE_OLLAMA_NL || "false") === "true";
const USE_FAISS_SIMILAR = (process.env.USE_FAISS_SIMILAR || "false") === "true";
const FAISS_TOP_K = Math.min(500, Math.max(5, Number(process.env.FAISS_TOP_K || 80)));

/**
 * Map planner intent → legacy PromptKind for clients that still branch on old strings.
 * @param {PlannerIntent} intent
 */
function intentToLegacyPromptKind(intent) {
  if (intent === "filter") return "filtering";
  if (intent === "similarity") return "similar_to_game";
  return "both";
}

/**
 * HTTP `filters` object → partial QuerySpec (same keys).
 * @param {import("../../shared/contracts.d.ts").GameFiltersPayload | Record<string, unknown> | null | undefined} f
 * @returns {Partial<QuerySpec>}
 */
export function filtersFromRequestBody(f) {
  if (!f || typeof f !== "object") return {};
  /** @type {Partial<QuerySpec>} */
  const raw = { ...f };
  return sanitizeQuerySpec(raw);
}

/**
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {string} bggId
 */
async function fetchGameDescription(runQuery, bggId) {
  const result = await runQuery(
    `
    MATCH (g:Game)
    WHERE toString(g.bgg_id) = $bid
    RETURN coalesce(g.description, '') AS d, coalesce(g.name, '') AS n
    LIMIT 1
    `,
    { bid: String(bggId) },
    "nlFetchDescription"
  );
  if (!result.records.length) {
    return { description: "", name: "" };
  }
  const d = result.records[0].get("d");
  const n = result.records[0].get("n");
  return { description: typeof d === "string" ? d : "", name: typeof n === "string" ? n : "" };
}

/**
 * @param {{ message: string, runQuery: (cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result> }} args
 * @returns {Promise<{ partial: Partial<QuerySpec>, nlParse: NlParseMeta | null }>}
 */
export async function parseNaturalLanguageMessage(args) {
  const message = String(args.message || "").trim();
  if (!USE_OLLAMA_NL || !message) {
    return {
      partial: messageToQuerySpec(message),
      nlParse: null
    };
  }

  /** @type {NlParseMeta} */
  const nlParse = { source: "ollama" };

  try {
    const plan = await planBoardGameQuery(message);
    nlParse.plannerIntent = plan.intent;
    nlParse.promptKind = intentToLegacyPromptKind(plan.intent);
    nlParse.plannerExplanation = plan.explanation || null;
    nlParse.similarToGame = plan.similarity_target;
    nlParse.faissIndex = "all_games";

    const fromFilters =
      plan.intent === "filter" || plan.intent === "hybrid" ? plannerFiltersToQuerySpec(plan.filters) : {};
    const fromTop = plannerResponseToQuerySpec(plan);
    /** @type {Partial<QuerySpec>} */
    let partial = sanitizeQuerySpec({
      ...fromFilters,
      ...fromTop,
      limit: plan.result_limit
    });
    applyPlannerRidgeDefaults(partial);

    if (plan.intent === "filter") {
      return { partial, nlParse };
    }

    const needsSimilar = plan.intent === "similarity" || plan.intent === "hybrid";
    const phrase = plan.similarity_target;

    if (needsSimilar && phrase && USE_FAISS_SIMILAR) {
      try {
        const resolved = await faissResolveName(phrase, 5);
        const top = resolved?.results?.[0];
        if (top && top.bgg_id != null) {
          const anchorBggId = String(top.bgg_id);
          nlParse.anchorBggId = anchorBggId;
          const { description, name } = await fetchGameDescription(args.runQuery, anchorBggId);
          const text = description && description.trim().length > 0 ? description : name;
          if (text && text.trim()) {
            const simParams = {
              text,
              top_k: FAISS_TOP_K,
              exclude_bgg_id: anchorBggId,
              category_slug: null
            };
            const sim = await faissSimilarByDescription(simParams);
            if (sim?.index_used === "category") {
              nlParse.faissIndex = "category";
            } else if (sim?.index_used === "all_games") {
              nlParse.faissIndex = "all_games";
            }
            const ids = (sim?.bgg_ids || []).map((x) => String(x));
            if (ids.length) {
              partial.bggIdAllowList = ids;
              nlParse.faissSimilarity = true;
            }
          }
        }
      } catch (e) {
        traceStep("nlQuery", "FAISS similarity failed (non-fatal)", {
          message: /** @type {Error} */ (e).message
        });
      }
    }

    const hasAllow = Boolean(partial.bggIdAllowList?.length);
    const anchorPhrase = phrase?.trim() || "";
    if (hasAllow) {
      partial.keyword = null;
    } else if (anchorPhrase) {
      partial.keyword = anchorPhrase;
    }

    return { partial, nlParse };
  } catch (err) {
    traceStep("nlQuery", "Ollama planner failed; keyword-only fallback", {
      message: /** @type {Error} */ (err).message
    });
    return {
      partial: messageToQuerySpec(message),
      nlParse: { source: "heuristic", promptKind: null, similarToGame: null }
    };
  }
}

/**
 * @param {object} args
 * @param {string} args.message
 * @param {import("../../shared/contracts.d.ts").GameFiltersPayload} [args.filters]
 * @param {number} [args.limit]
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} args.runQuery
 */
export async function buildQuerySpecFromNaturalLanguage(args) {
  const message = String(args.message || "").trim();
  const filters = args.filters || {};
  const hasMessage = Boolean(message);
  const baseFilters = hasMessage ? {} : filtersFromRequestBody(filters);
  const presetFromUi = hasMessage ? undefined : /** @type {QueryPresetId | null | undefined} */ (filters.preset ?? null);

  if (!USE_OLLAMA_NL || !message) {
    const fromMsg = messageToQuerySpec(message);
    const merged = mergeQuerySpec(
      { ...baseFilters, limit: args.limit ?? 40 },
      fromMsg,
      presetFromUi
    );
    return { spec: merged, nlParse: /** @type {NlParseMeta | null} */ (null) };
  }

  const { partial, nlParse } = await parseNaturalLanguageMessage({
    message,
    runQuery: args.runQuery
  });
  const merged = mergeQuerySpec(
    { ...baseFilters, limit: args.limit ?? 40 },
    partial,
    presetFromUi
  );
  return { spec: merged, nlParse };
}
