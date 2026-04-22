/**
 * NL → QuerySpec: Ollama (classify + filter A + filter B + reconcile), optional FAISS similarity.
 */
import {
  classifyPromptKind,
  filterStepActiveKinds,
  filterStepExtractValues,
  sanitizeQuerySpec,
  selectFaissCategorySlug,
  reconcileQuerySpecFromMessage
} from "./ollamaNlp.js";
import {
  faissResolveName,
  faissSimilarByDescription,
  faissListCategorySlugs
} from "./faissClient.js";
import { mergeQuerySpec, messageToQuerySpec } from "./searchQuery.js";
import { traceStep } from "./trace.js";

/** @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec */
/** @typedef {import("../../shared/contracts.d.ts").NlParseMeta} NlParseMeta */
/** @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId */

export const USE_OLLAMA_NL = (process.env.USE_OLLAMA_NL || "false") === "true";
const USE_FAISS_SIMILAR = (process.env.USE_FAISS_SIMILAR || "false") === "true";
const FAISS_TOP_K = Math.min(500, Math.max(5, Number(process.env.FAISS_TOP_K || 80)));

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
 * Merge explicit UI filters (same shape as bodyToQuerySpec).
 * @param {import("../../shared/contracts.d.ts").RecommendRequestBody["filters"] | Record<string, unknown>} f
 */
function explicitFiltersToPartial(f) {
  const fl = f || {};
  return {
    keyword: fl.keyword != null && fl.keyword !== "" ? String(fl.keyword) : undefined,
    players: fl.players != null ? Number(fl.players) : undefined,
    maxTime: fl.maxTime != null ? Number(fl.maxTime) : undefined,
    maxPrice: fl.maxPrice != null ? Number(fl.maxPrice) : undefined,
    minRating: fl.minRating != null ? Number(fl.minRating) : undefined,
    sort: fl.sort,
    preset: fl.preset ?? undefined
  };
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
    const route = await classifyPromptKind(message);
    nlParse.promptKind = route.promptKind;
    nlParse.similarToGame = route.similarToGame;

    /** @type {Partial<QuerySpec>} */
    let partial = {};

    // Only "filtering" and "both" use filter A/B/reconcile. Pure "similar_to_game" (e.g. "Games like
    // Brass Birmingham") has no table filters; running those steps invented keyword/preset and
    // broke the FAISS + search allow-list path.
    const needsFilterExtract = route.promptKind === "filtering" || route.promptKind === "both";

    if (needsFilterExtract) {
      const active = await filterStepActiveKinds(message);
      const rawPartial = await filterStepExtractValues(message, active);
      const merged = sanitizeQuerySpec(rawPartial);
      const reconciled = await reconcileQuerySpecFromMessage(message, merged);
      partial = { ...merged, ...reconciled };
    }

    if (route.promptKind === "filtering") {
      return { partial, nlParse };
    }

    const needsSimilar = route.promptKind === "similar_to_game" || route.promptKind === "both";

    /** @type {string | null} */
    let faissCategorySlug = null;

    if (needsSimilar && USE_FAISS_SIMILAR) {
      try {
        const categorySlugAllowlist = await faissListCategorySlugs();
        faissCategorySlug = await selectFaissCategorySlug(message, categorySlugAllowlist);
        nlParse.faissCategorySlug = faissCategorySlug ?? null;
        nlParse.faissIndex = faissCategorySlug ? "category" : "all_games";
      } catch (slugErr) {
        traceStep("nlQuery", "category slug list / selection skipped (non-fatal)", {
          message: /** @type {Error} */ (slugErr).message
        });
        nlParse.faissIndex = "all_games";
      }
    }

    if (needsSimilar) {
      const phrase = route.similarToGame;
      if (!phrase || !phrase.trim()) {
        traceStep("nlQuery", "similar leg: missing similarToGame after route");
      } else if (USE_FAISS_SIMILAR) {
        try {
          const resolved = await faissResolveName(phrase, 5);
          const top = resolved?.results?.[0];
          if (top && top.bgg_id != null) {
            const anchorBggId = String(top.bgg_id);
            nlParse.anchorBggId = anchorBggId;
            const { description, name } = await fetchGameDescription(args.runQuery, anchorBggId);
            const text = description && description.trim().length > 0 ? description : name;
            if (text && text.trim()) {
              /** @type {Record<string, unknown>} */
              const simParams = {
                text,
                top_k: FAISS_TOP_K,
                exclude_bgg_id: anchorBggId,
                category_slug: faissCategorySlug
              };
              let sim;
              try {
                sim = await faissSimilarByDescription(simParams);
              } catch (shardErr) {
                if (faissCategorySlug) {
                  traceStep("nlQuery", "FAISS category shard failed; retry all_games", {
                    message: /** @type {Error} */ (shardErr).message
                  });
                  nlParse.faissCategorySlug = null;
                  nlParse.faissIndex = "all_games";
                  sim = await faissSimilarByDescription({
                    ...simParams,
                    category_slug: null
                  });
                } else {
                  throw shardErr;
                }
              }
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
      const anchorPhrase = route.similarToGame?.trim() || "";
      if (hasAllow) {
        partial.keyword = null;
      } else if (anchorPhrase) {
        partial.keyword = anchorPhrase;
      }
    }

    return { partial, nlParse };
  } catch (err) {
    traceStep("nlQuery", "Ollama NL failed; keyword-only fallback (no Ollama)", {
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
 * @param {import("../../shared/contracts.d.ts").RecommendRequestBody["filters"]} [args.filters]
 * @param {number} [args.limit]
 * @param {(cypher: string, params: object, label?: string) => Promise<import("neo4j-driver").Result>} args.runQuery
 */
export async function buildQuerySpecFromNaturalLanguage(args) {
  const message = String(args.message || "").trim();
  const filters = args.filters || {};
  /** When the user is asking in natural language, the spec must come from the text (Ollama / keyword), not from filter-bar defaults. */
  const hasMessage = Boolean(message);
  const baseFilters = hasMessage ? {} : explicitFiltersToPartial(filters);
  const presetFromUi = hasMessage
    ? undefined
    : /** @type {QueryPresetId | null | undefined} */ (filters.preset ?? null);

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
