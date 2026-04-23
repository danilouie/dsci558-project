import neo4j from "neo4j-driver";
import { isTraceEnabled, traceStep } from "./trace.js";
import { gameNodeToSummary } from "./gameMapper.js";

/**
 * @typedef {import("../../shared/contracts.d.ts").QuerySpec} QuerySpec
 * @typedef {import("../../shared/contracts.d.ts").QueryPresetId} QueryPresetId
 * @typedef {import("../../shared/contracts.d.ts").SearchSortField} SearchSortField
 * @typedef {import("../../shared/contracts.d.ts").SearchHit} SearchHit
 * @typedef {import("../../shared/contracts.d.ts").SearchExplain} SearchExplain
 */

const DEFAULT_LIMIT = 40;
const CANDIDATE_CAP = 400;

/**
 * Convert driver params to JSON-friendly values for logging / Neo4j Browser :params.
 * @param {object} params
 * @returns {Record<string, unknown>}
 */
function serializeParamsForLog(params) {
  /** @param {unknown} v */
  const walk = (v) => {
    if (v == null) return v;
    if (typeof v === "object" && v !== null && "toNumber" in v && typeof /** @type {{ toNumber?: () => number }} */ (v).toNumber === "function") {
      try {
        return /** @type {{ toNumber: () => number }} */ (v).toNumber();
      } catch {
        return String(v);
      }
    }
    if (Array.isArray(v)) return v.map(walk);
    return v;
  };
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const [k, val] of Object.entries(params)) {
    out[k] = walk(val);
  }
  return out;
}

/**
 * Log full Cypher + params when TRACE_STEPS is enabled (default on).
 * Paste the query into Neo4j Browser, then run `:params` with the printed JSON (or substitute manually).
 * @param {string} queryText
 * @param {object} params
 */
function traceCypherSearch(queryText, params) {
  if (!isTraceEnabled()) return;
  const t = new Date().toISOString();
  const serial = serializeParamsForLog(params);
  console.log(`[${t}] [kg-trace:search] --- CYPHER_SEARCH (copy into Neo4j Browser) ---`);
  console.log(queryText.trim());
  console.log(`[${t}] [kg-trace:search] --- Params JSON (Browser: :params then paste object, or use $param names in query) ---`);
  console.log(JSON.stringify(serial, null, 2));
}

const PRESET_DEFAULTS = /** @type {Record<NonNullable<import("../../shared/contracts.d.ts").QueryPresetId>, Partial<QuerySpec>>} */ ({
  best_under_budget: {
    maxPrice: 30,
    minRating: 6.5,
    sort: "mean_price",
    sortDirection: "asc"
  },
  value_for_price: { sort: "rating_per_dollar", sortDirection: "desc" },
  highly_rated_cheap: {
    maxPrice: 30,
    minRating: 7.5,
    sort: "rating",
    sortDirection: "desc"
  },
  overpriced: { overpricedOnly: true, sort: "pred_avg_quality", sortDirection: "asc" },
  undervalued: { undervaluedOnly: true, sort: "pred_avg_quality", sortDirection: "desc" },
  high_want_low_own: {
    minWants: 1,
    sort: "want_minus_own",
    sortDirection: "desc"
  },
  frequently_traded: { sort: "wtt", sortDirection: "desc" },
  rating_per_dollar: { sort: "rating_per_dollar", sortDirection: "desc" },
  composite_demo: {
    maxPrice: 50,
    minWants: 1,
    minRating: 7.0,
    sort: "rating",
    sortDirection: "desc"
  }
});

/**
 * Ridge thresholds for “undervalued” / “overpriced” presets — applied as bounds on `pred_avg_quality`
 * (see SCHEMA.md `:Game`). Unset env = no numeric bound; preset still requires `pred_avg_quality` non-null.
 */
export function getThresholds() {
  return {
    predAvgQualityUndervaluedMin: toFloatEnvOptional(process.env.PRED_AVG_QUALITY_UNDERVALUED_MIN),
    predAvgQualityOverpricedMax: toFloatEnvOptional(process.env.PRED_AVG_QUALITY_OVERPRICED_MAX)
  };
}

function toFloatEnvOptional(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {Partial<QuerySpec> | null | undefined} spec
 * @param {import("../../shared/contracts.d.ts").QueryPresetId | null | undefined} preset
 * @returns {QuerySpec}
 */
export function applyPresetMerge(spec, preset) {
  const s = { ...(spec || {}) };
  if (preset) {
    const p = PRESET_DEFAULTS[preset] || {};
    s.preset = preset;
    s.sort = s.sort ?? p.sort;
    s.sortDirection = s.sortDirection ?? p.sortDirection;
    s.maxPrice = s.maxPrice ?? p.maxPrice ?? null;
    s.minPrice = s.minPrice ?? p.minPrice ?? null;
    s.minRating = s.minRating ?? p.minRating ?? null;
    s.minWants = s.minWants ?? p.minWants ?? null;
    s.minOwns = s.minOwns ?? p.minOwns ?? null;
    s.undervaluedOnly = s.undervaluedOnly || Boolean(p.undervaluedOnly);
    s.overpricedOnly = s.overpricedOnly || Boolean(p.overpricedOnly);
  } else {
    s.preset = s.preset ?? null;
  }
  if (!s.sort) s.sort = "rating";
  if (!s.sortDirection) s.sortDirection = "desc";
  return s;
}

/**
 * @param {string} message
 * @returns {Partial<QuerySpec>}
 */
/**
 * Non-Ollama fallback: keyword search plus light hints for ridge undervalued/overpriced and traded.
 * Ridge: "undervalued" → high `pred_avg_quality` (not BGG rating). "Overpriced" → low pred_avg_quality.
 * @param {string} message
 * @returns {Partial<QuerySpec>}
 */
export function messageToQuerySpec(message) {
  const raw = String(message || "").trim();
  if (!raw) return {};
  const kw = raw.length > 120 ? raw.slice(0, 120) : raw;
  /** @type {Partial<QuerySpec>} */
  const out = { keyword: kw };

  if (/\bundervalued\b/i.test(raw)) {
    out.undervaluedOnly = true;
  }
  if (/\boverpriced\b/i.test(raw)) {
    out.overpricedOnly = true;
  }
  if (
    /\b(frequently traded|traded a lot|traded lots)\b/i.test(raw) ||
    /\bhigh\s+wtt\b/i.test(raw)
  ) {
    out.preset = "frequently_traded";
  }

  if (out.undervaluedOnly && !out.overpricedOnly && out.preset !== "frequently_traded") {
    out.sort = "pred_avg_quality";
    out.sortDirection = "desc";
  } else if (out.overpricedOnly && !out.undervaluedOnly) {
    out.sort = "pred_avg_quality";
    out.sortDirection = "asc";
  }

  return out;
}

/**
 * Merges UI / API "base" filters (often defaults) with message-derived (NL) fields.
 * The message layer wins for any key it sets, so a prompt like "under 40" is not replaced
 * by the filter bar's maxPrice until the user clears or changes the text.
 *
 * @param {Partial<QuerySpec>} base - explicit filters from UI or `query` (lower priority)
 * @param {Partial<QuerySpec>} messageDerived - Ollama partial or `messageToQuerySpec` (wins on conflict)
 * @param {import("../../shared/contracts.d.ts").QueryPresetId | null | undefined} presetFromFilter
 */
export function mergeQuerySpec(base, messageDerived, presetFromFilter) {
  const merged = { ...base };
  for (const [k, v] of Object.entries(messageDerived)) {
    if (v !== undefined) {
      merged[/** @type {keyof import("../../shared/contracts.d.ts").QuerySpec} */ (k)] = v;
    }
  }
  const hasMsgPreset = Object.hasOwn(/** @type {object} */ (messageDerived), "preset");
  const p = hasMsgPreset ? messageDerived.preset : base.preset ?? presetFromFilter;
  return applyPresetMerge(merged, p);
}

function toNum(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * @param {import("neo4j-driver").Node} node
 * @param {import("../../shared/contracts.d.ts").SearchExplain} ex
 * @param {import("../../shared/contracts.d.ts").SearchSortField} sort
 */
function nodeToGameWithExplain(node, ex, sort) {
  const est = ex.meanPrice;
  const base = gameNodeToSummary(node, { estimatedPrice: est != null && Number.isFinite(Number(est)) ? Number(est) : null });
  return {
    ...base,
    searchExplain: { ...ex, sort, preset: ex.preset }
  };
}

/**
 * @param {import("neo4j-driver").Integer | number | null} v
 * @returns {number}
 */
function toIntish(v) {
  if (v == null) return 0;
  if (typeof v === "object" && v !== null && "toNumber" in v) return toIntish(/** @type {any} */ (v).toNumber());
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

/**
 * @param {import("neo4j-driver").Result} result
 * @param {QuerySpec} spec
 */
function mapSearchRecords(result, spec) {
  /** @type {import("../../shared/contracts.d.ts").SearchHit[]} */
  const hits = [];
  for (const rec of result.records) {
    const node = rec.get("g");
    const wants = toIntish(rec.get("wants"));
    const wtb = toIntish(rec.get("wtb"));
    const wtt = toIntish(rec.get("wtt"));
    const owns = toIntish(rec.get("owns"));
    const rnk = toNum(neoToNum(rec.get("rankValue")));
    const rating = toNum(neoToNum(rec.get("rating")));
    const meanP = toNum(neoToNum(rec.get("meanP")));
    const pDateVal = rec.get("pDate");
    const pDate = formatNeoDate(pDateVal);
    const rpd = meanP != null && meanP > 0 && rating != null ? rating / meanP : null;

    const explain = {
      meanPrice: meanP,
      priceDate: pDate,
      wants: wants || 0,
      wtb: wtb || 0,
      wtt: wtt || 0,
      owns: owns || 0,
      rankValue: rnk,
      valueScore: null,
      overpriced: null,
      undervalued: null,
      ratingPerDollar: rpd,
      priceDropVsWindowMin: null,
      hasValueScoreProp: false,
      sort: spec.sort,
      preset: spec.preset ?? null
    };

    hits.push({
      game: nodeToGameWithExplain(node, explain, spec.sort),
      explain
    });
  }

  const st = spec.sort;
  const dir = spec.sortDirection || "desc";

  hits.sort((A, B) => {
    const a = A.explain;
    const b = B.explain;
    let x = 0;
    switch (st) {
      case "mean_price":
        x = (a.meanPrice ?? 999) - (b.meanPrice ?? 999);
        break;
      case "geek_rating":
        x = (A.game.rating || 0) - (B.game.rating || 0);
        break;
      case "rank_value_asc": {
        const ar = a.rankValue ?? 99999;
        const br = b.rankValue ?? 99999;
        x = ar - br;
        break;
      }
      case "rating_per_dollar":
        x = (a.ratingPerDollar || 0) - (b.ratingPerDollar || 0);
        break;
      case "want_minus_own":
        x = a.wants - a.owns - (b.wants - b.owns);
        break;
      case "wtt_to_wants":
        x = (a.wants > 0 ? a.wtt / a.wants : 0) - (b.wants > 0 ? b.wtt / b.wants : 0);
        break;
      case "value_score":
      case "pred_avg_quality": {
        const ap = A.game.predAvgQuality;
        const bp = B.game.predAvgQuality;
        const av = ap != null && Number.isFinite(ap) ? ap : -Infinity;
        const bv = bp != null && Number.isFinite(bp) ? bp : -Infinity;
        x = av - bv;
        break;
      }
      case "wants":
        x = a.wants - b.wants;
        break;
      case "wtt":
        x = a.wtt - b.wtt;
        break;
      case "price_drop":
        x = (a.priceDropVsWindowMin || 0) - (b.priceDropVsWindowMin || 0);
        break;
      case "rating":
      default:
        x = (A.game.rating || 0) - (B.game.rating || 0);
        if (x === 0) x = (A.game.usersRated || 0) - (B.game.usersRated || 0);
    }
    if (dir === "asc") return x > 0 ? 1 : x < 0 ? -1 : 0;
    return x < 0 ? 1 : x > 0 ? -1 : 0;
  });

  return hits;
}

/**
 * @param {any} v
 */
function neoToNum(v) {
  if (v == null) return null;
  if (typeof v === "object" && v != null && "toNumber" in v) {
    try {
      return toNum(/** @type {import("neo4j-driver").Integer} */ (v).toNumber());
    } catch {
      return null;
    }
  }
  return toNum(v);
}

/**
 * @param {any} d
 * @returns {string | null}
 */
function formatNeoDate(d) {
  if (d == null) return null;
  if (typeof d === "string") return d;
  if (typeof d === "object" && d != null && "year" in d) {
    const o = d;
    const y = o.year;
    const mo = o.month;
    const day = o.day;
    if (y && mo && day) {
      return `${y.low ?? y}-${String(mo.low ?? mo).padStart(2, "0")}-${String(day.low ?? day).padStart(2, "0")}`;
    }
  }
  if (d.toString) return String(d.toString());
  return null;
}

const CYPHER_SEARCH = `
  MATCH (g:Game)
  WHERE (size($bggAllow) = 0 OR toString(g.bgg_id) IN $bggAllow)
    AND ($kw IS NULL OR toLower(coalesce(g.name, "")) CONTAINS toLower($kw))
    AND ($yearMin IS NULL OR coalesce(toInteger(g.yearpublished), toInteger(g.year), -1) >= $yearMin)
    AND ($yearMax IS NULL OR coalesce(toInteger(g.yearpublished), toInteger(g.year), 999999) <= $yearMax)
    AND ($seatN IS NULL OR (
      toInteger(coalesce(g.min_players, 1)) <= $seatN AND
      toInteger(coalesce(g.max_players, 999)) >= $seatN
    ))
    AND (($pfMin IS NULL AND $pfMax IS NULL) OR (
      toInteger(coalesce(g.min_players, 1)) <= coalesce($pfMax, 9999) AND
      toInteger(coalesce(g.max_players, 999)) >= coalesce($pfMin, 1)
    ))
    AND ($maxTime IS NULL OR toInteger(coalesce(g.max_playtime, 9999)) <= $maxTime)
    AND ($minPt IS NULL OR toInteger(coalesce(g.min_playtime, 0)) >= $minPt)
    AND ($maxMinAge IS NULL OR toInteger(coalesce(g.min_age, 99)) <= $maxMinAge)
    AND ($expF IS NULL OR coalesce(g.is_expansion, false) = $expF)
    AND (toInteger($nnPQ) = 0 OR g.pred_avg_quality IS NOT NULL)
    AND ($minPQ IS NULL OR (g.pred_avg_quality IS NOT NULL AND toFloat(g.pred_avg_quality) >= toFloat($minPQ)))
    AND ($maxPQ IS NULL OR (g.pred_avg_quality IS NOT NULL AND toFloat(g.pred_avg_quality) <= toFloat($maxPQ)))
  WITH g, coalesce(g.categories, $emptyList) AS cats, coalesce(g.mechanisms, $emptyList) AS mechs
  WHERE (size($catNeedles) = 0 OR ANY(c IN cats WHERE toLower(trim(toString(c))) IN $catNeedles))
    AND (size($mechNeedles) = 0 OR ANY(m IN mechs WHERE toLower(trim(toString(m))) IN $mechNeedles))
  WITH g
  WHERE ($minC IS NULL OR toFloat(g.complexity) >= toFloat($minC))
    AND ($maxC IS NULL OR toFloat(g.complexity) <= toFloat($maxC))
  OPTIONAL MATCH (g)-[:HAS_RANK]->(rk:Rank)
  WITH g, coalesce(toInteger(rk.rank_value), toInteger(g.\`rank\`), 99999) AS rankValue
  OPTIONAL MATCH (g)-[:HAS_PRICE_POINT]->(p:PricePoint)
  WITH g, rankValue, p ORDER BY p.date DESC
  WITH g, rankValue, collect(p) AS pList
  WITH g, rankValue, CASE WHEN size(pList) > 0 THEN pList[0] ELSE null END AS latest
  WITH g, rankValue, latest,
    count { (u:User)-[:WANTS]->(g) } AS wants,
    count { (u:User)-[:WANTS_TO_BUY]->(g) } AS wtb,
    count { (u:User)-[:WANTS_TO_TRADE]->(g) } AS wtt,
    count { (u:User)-[:OWNS]->(g) } AS owns
  WHERE ($usr IS NULL OR NOT EXISTS( (:User {username: $usr})-[:OWNS]->(g) ))
  WITH g, rankValue, latest, wants, wtb, wtt, owns,
    coalesce(toFloat(g.geek_rating), toFloat(g.avg_rating), toFloat(g.bayesaverage), toFloat(g.average), 0.0) AS rating
  WITH g, rankValue, latest, wants, wtb, wtt, owns, rating,
    CASE WHEN latest IS NULL THEN null ELSE toFloat(latest.mean_price) END AS meanP,
    (CASE WHEN latest IS NULL THEN null ELSE latest.date END) AS pDate
  WHERE ($maxP IS NULL OR (meanP IS NOT NULL AND meanP <= toFloat($maxP)))
    AND ($minP IS NULL OR (meanP IS NOT NULL AND meanP >= toFloat($minP)))
    AND ($minR IS NULL OR (rating IS NOT NULL AND rating >= toFloat($minR)))
    AND ($minW IS NULL OR toInteger(wants) >= toInteger($minW))
    AND ($minO IS NULL OR toInteger(owns) >= toInteger($minO))
  RETURN
    g,
    latest,
    toInteger(wants) AS wants,
    toInteger(wtb) AS wtb,
    toInteger(wtt) AS wtt,
    toInteger(owns) AS owns,
    toFloat(rankValue) AS rankValue,
    toFloat(rating) AS rating,
    meanP AS meanP,
    pDate
  LIMIT $cap
  `;

/**
 * @param {import("neo4j-driver").Driver} _driver
 * @param {string} _database
 * @param {(cypher: string, params: object) => Promise<import("neo4j-driver").Result>} runQuery
 * @param {QuerySpec} spec0
 * @param {import("neo4j-driver")} [neo4jRef]
 */
export async function runSearchQuery(_driver, _database, runQuery, spec0, neo4jRef = neo4j) {
  traceStep("search", "1. runSearchQuery start", { preset: spec0.preset, sort: spec0.sort, maxPrice: spec0.maxPrice });
  const t = getThresholds();
  const spec = applyPresetMerge(spec0, spec0.preset);
  traceStep("search", "2. after applyPresetMerge", {
    sort: spec.sort,
    sortDirection: spec.sortDirection,
    maxPrice: spec.maxPrice,
    minRating: spec.minRating,
    undervaluedOnly: spec.undervaluedOnly,
    overpricedOnly: spec.overpricedOnly,
    minWants: spec.minWants
  });
  const finalLimit = Math.min(spec.limit || DEFAULT_LIMIT, 200);
  const capN = CANDIDATE_CAP;

  /** @type {number | null} */
  let minPQ = spec.minPredAvgQuality != null ? spec.minPredAvgQuality : null;
  /** @type {number | null} */
  let maxPQ = spec.maxPredAvgQuality != null ? spec.maxPredAvgQuality : null;

  if (spec.undervaluedOnly && t.predAvgQualityUndervaluedMin != null) {
    minPQ =
      minPQ != null ? Math.max(minPQ, t.predAvgQualityUndervaluedMin) : t.predAvgQualityUndervaluedMin;
  }
  if (spec.overpricedOnly && t.predAvgQualityOverpricedMax != null) {
    maxPQ =
      maxPQ != null ? Math.min(maxPQ, t.predAvgQualityOverpricedMax) : t.predAvgQualityOverpricedMax;
  }

  const nnPQNeed =
    (spec.undervaluedOnly && minPQ == null && t.predAvgQualityUndervaluedMin == null) ||
    (spec.overpricedOnly && maxPQ == null && t.predAvgQualityOverpricedMax == null);

  const catNeedles = (spec.categoryContains || []).map((s) => String(s).toLowerCase().trim()).filter(Boolean);
  const mechNeedles = (spec.mechanismContains || []).map((s) => String(s).toLowerCase().trim()).filter(Boolean);
  const kwParam =
    spec.keyword == null || String(spec.keyword).trim() === "" ? null : String(spec.keyword).trim();

  const seatN =
    spec.supportsPlayerCount != null && Number.isFinite(Number(spec.supportsPlayerCount))
      ? Number(spec.supportsPlayerCount)
      : spec.players != null && Number.isFinite(Number(spec.players))
        ? Number(spec.players)
        : null;

  const bggAllow =
    spec.bggIdAllowList && spec.bggIdAllowList.length
      ? spec.bggIdAllowList.map((x) => String(x))
      : [];

  const params = {
    bggAllow: bggAllow,
    kw: kwParam,
    seatN: seatN != null ? neo4jRef.int(Math.round(seatN)) : null,
    pfMin: spec.filterMinPlayers != null ? neo4jRef.int(Math.round(spec.filterMinPlayers)) : null,
    pfMax: spec.filterMaxPlayers != null ? neo4jRef.int(Math.round(spec.filterMaxPlayers)) : null,
    yearMin: spec.minYear != null ? neo4jRef.int(Math.round(spec.minYear)) : null,
    yearMax: spec.maxYear != null ? neo4jRef.int(Math.round(spec.maxYear)) : null,
    minPt: spec.minPlaytime != null ? neo4jRef.int(Math.round(spec.minPlaytime)) : null,
    maxMinAge: spec.maxMinAge != null ? neo4jRef.int(Math.round(spec.maxMinAge)) : null,
    expF: spec.isExpansion === true || spec.isExpansion === false ? spec.isExpansion : null,
    minPQ: minPQ != null ? minPQ : null,
    maxPQ: maxPQ != null ? maxPQ : null,
    nnPQ: neo4jRef.int(nnPQNeed ? 1 : 0),
    maxTime: spec.maxTime != null ? neo4jRef.int(spec.maxTime) : null,
    minC: spec.minComplexity != null ? spec.minComplexity : null,
    maxC: spec.maxComplexity != null ? spec.maxComplexity : null,
    emptyList: [],
    catNeedles: catNeedles,
    mechNeedles: mechNeedles,
    maxP: spec.maxPrice != null ? spec.maxPrice : null,
    minP: spec.minPrice != null ? spec.minPrice : null,
    minR: spec.minRating != null ? spec.minRating : null,
    minW: spec.minWants != null ? neo4jRef.int(spec.minWants) : null,
    minO: spec.minOwns != null ? neo4jRef.int(spec.minOwns) : null,
    usr: spec.usernameExcludesOwns || null,
    cap: neo4jRef.int(capN)
  };

  traceStep("search", "3. cypher params ready", {
    hasKeyword: params.kw != null,
    bggAllowCount: bggAllow.length,
    catNeedles: params.catNeedles.length,
    mechNeedles: params.mechNeedles.length,
    candidateCap: capN,
    nnPQ: nnPQNeed,
    predBounds: { minPQ, maxPQ },
    ridgeThresholds: {
      predAvgQualityUndervaluedMin: t.predAvgQualityUndervaluedMin,
      predAvgQualityOverpricedMax: t.predAvgQualityOverpricedMax
    }
  });
  traceCypherSearch(CYPHER_SEARCH, params);
  const t0 = Date.now();
  const result = await runQuery(CYPHER_SEARCH, params, "CYPHER_SEARCH");
  traceStep("search", "4. cypher returned", { ms: Date.now() - t0, rawRecordCount: result.records.length });
  const allHits = mapSearchRecords(result, spec);
  traceStep("search", "5. after sort+map", { sortedCount: allHits.length, returnLimit: finalLimit });
  const sliced = allHits.slice(0, finalLimit);
  traceStep("search", "6. runSearchQuery end", { hitsReturned: sliced.length, top: sliced[0]?.game?.name ?? null });
  return { hits: sliced, spec };
}
