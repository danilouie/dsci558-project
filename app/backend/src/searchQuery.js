import neo4j from "neo4j-driver";
import { traceStep } from "./trace.js";
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
  overpriced: { overpricedOnly: true, sort: "mean_price", sortDirection: "desc" },
  undervalued: { undervaluedOnly: true, sort: "value_score", sortDirection: "desc" },
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

export function getThresholds() {
  const u = process.env.VALUE_SCORE_UNDERVAlUED_MIN || process.env.VALUE_SCORE_UNDERVALUED_MIN;
  const rpu = process.env.RATING_PER_DOLLAR_UNDERVAlUED_MIN || process.env.RATING_PER_DOLLAR_UNDERVALUED_MIN;
  return {
    valueUndervaluedMin: toFloatEnv(u, 0),
    valueOverpricedMax: toFloatEnv(process.env.VALUE_SCORE_OVERPRICED_MAX, 0),
    rpdUndervaluedMin: toFloatEnv(rpu, 0.2),
    rpdOverpricedMax: toFloatEnv(process.env.RATING_PER_DOLLAR_OVERPRICED_MAX, 0.08)
  };
}

function toFloatEnv(v, d) {
  if (v == null || v === "") return d;
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
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
 * Non-Ollama fallback: pass the message through as a keyword only (no regex heuristics).
 * Does not set sort/sortDirection so merged specs keep UI or default ordering from applyPresetMerge.
 * @param {string} message
 * @returns {Partial<QuerySpec>}
 */
export function messageToQuerySpec(message) {
  const raw = String(message || "").trim();
  if (!raw) return {};
  const kw = raw.length > 120 ? raw.slice(0, 120) : raw;
  return { keyword: kw };
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
    const valueScore = toNum(neoToNum(rec.get("vScore")));
    const gOverP = rec.get("gOver");
    const gUnderP = rec.get("gUnder");
    const wMin = toNum(neoToNum(rec.get("wMin")));
    const hasV = rec.get("hasVScore");
    const rpd = meanP != null && meanP > 0 && rating != null ? rating / meanP : null;
    const priceDrop = wMin != null && meanP != null ? wMin - meanP : null;
    const hasVScore = hasV === true;

    const overpriced = typeof gOverP === "boolean" ? gOverP : gOverP == null ? null : null;
    const undervalued = typeof gUnderP === "boolean" ? gUnderP : gUnderP == null ? null : null;

    const explain = {
      meanPrice: meanP,
      priceDate: pDate,
      wants: wants || 0,
      wtb: wtb || 0,
      wtt: wtt || 0,
      owns: owns || 0,
      rankValue: rnk,
      valueScore: valueScore,
      overpriced,
      undervalued,
      ratingPerDollar: rpd,
      priceDropVsWindowMin: priceDrop,
      hasValueScoreProp: hasVScore,
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
        x = (a.valueScore ?? 0) - (b.valueScore ?? 0);
        break;
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
    AND ($players IS NULL OR toInteger(coalesce(g.min_players, g.minplayers, 0)) <= $players)
    AND ($maxTime IS NULL OR toInteger(coalesce(g.playingtime, g.maxplaytime, g.play_time, 9999)) <= $maxTime)
  WITH g, coalesce(g.categories, $emptyList) AS cats, coalesce(g.mechanisms, $emptyList) AS mechs
  WHERE (size($catNeedles) = 0 OR ANY(needle IN $catNeedles WHERE ANY(
        c IN cats WHERE toLower(toString(c)) CONTAINS needle
      )))
    AND (size($mechNeedles) = 0 OR ANY(needle IN $mechNeedles WHERE ANY(
        c IN mechs WHERE toLower(toString(c)) CONTAINS needle
      )))
  WITH g
  WHERE ($minC IS NULL OR toFloat(g.complexity) >= toFloat($minC))
    AND ($maxC IS NULL OR toFloat(g.complexity) <= toFloat($maxC))
  OPTIONAL MATCH (g)-[:HAS_RANK]->(rk:Rank)
  WITH g, coalesce(
    toInteger(rk.rank_value),
    toInteger(g.\`rank\`),
    99999
  ) AS rankValue
  OPTIONAL MATCH (g)-[:HAS_PRICE_POINT]->(p:PricePoint)
  WITH g, rankValue, p ORDER BY p.date DESC
  WITH g, rankValue, collect(p) AS pList
  WITH g, rankValue, CASE WHEN size(pList) > 0 THEN pList[0] ELSE null END AS latest
  OPTIONAL MATCH (g)-[:HAS_PRICE_POINT]->(pW:PricePoint)
  WHERE pW IS NOT NULL AND pW.date >= (date() - duration({ days: $winDays }))
  WITH g, rankValue, latest, min(toFloat(pW.mean_price)) AS wMin
  WITH g, rankValue, latest, wMin
  , count { (u:User)-[:WANTS]->(g) } AS wants
  , count { (u:User)-[:WANTS_TO_BUY]->(g) } AS wtb
  , count { (u:User)-[:WANTS_TO_TRADE]->(g) } AS wtt
  , count { (u:User)-[:OWNS]->(g) } AS owns
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns
  WHERE ($usr IS NULL OR NOT EXISTS( (:User {username: $usr})-[:OWNS]->(g) ))
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns,
    coalesce(toFloat(g.geek_rating), toFloat(g.bayesavg), toFloat(g.average), 0.0) AS rating
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating,
    CASE WHEN latest IS NULL THEN null ELSE toFloat(latest.mean_price) END AS meanP
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP
  , coalesce(toFloat(g.value_score), null) AS vScore
  , (g.value_score IS NOT NULL) AS hasVScore
  , (CASE WHEN latest IS NULL THEN null ELSE latest.date END) AS pDate
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP, vScore, hasVScore, pDate
  WHERE ($maxP IS NULL OR (meanP IS NOT NULL AND meanP <= toFloat($maxP)))
    AND ($minP IS NULL OR (meanP IS NOT NULL AND meanP >= toFloat($minP)))
    AND ($minR IS NULL OR (rating IS NOT NULL AND rating >= toFloat($minR)))
    AND ($minW IS NULL OR toInteger(wants) >= toInteger($minW))
    AND ($minO IS NULL OR toInteger(owns) >= toInteger($minO))
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP, vScore, hasVScore, pDate
  WHERE toInteger($undF) = 0
    OR coalesce(g.undervalued, false) = true
    OR ( $tUndS > 0.0 AND vScore IS NOT NULL AND vScore >= toFloat($tUndS))
    OR ( toInteger($needRpdU) = 1 AND meanP IS NOT NULL AND meanP > 0
        AND (rating / meanP) >= toFloat($rpdUmin) )
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP, vScore, hasVScore, pDate
  WHERE toInteger($ovF) = 0
    OR coalesce(g.overpriced, false) = true
    OR ( $tOvrS > 0.0 AND vScore IS NOT NULL AND vScore <= toFloat($tOvrS))
    OR ( toInteger($needRpdO) = 1 AND meanP IS NOT NULL AND meanP > 0
        AND (rating / meanP) <= toFloat($rpdOmax) )
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP, vScore, hasVScore, pDate
  WHERE toInteger($wDropF) = 0 OR wMin IS NULL OR meanP IS NULL OR wMin - meanP >= toFloat($minPdrop)
  WITH g, rankValue, latest, wMin, wants, wtb, wtt, owns, rating, meanP, vScore, hasVScore, pDate
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
    pDate,
    vScore,
    g.overpriced AS gOver,
    g.undervalued AS gUnder,
    wMin,
    toBoolean(hasVScore) AS hasVScore
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
  const windowDays = Math.max(1, spec.priceWindowDays != null ? spec.priceWindowDays : 90);
  const minPdrop = spec.minPriceDrop != null && spec.minPriceDrop > 0 ? spec.minPriceDrop : 0.0;
  const capN = CANDIDATE_CAP;

  const tUndS = t.valueUndervaluedMin > 0 ? t.valueUndervaluedMin : 0.0;
  const tOvrS = t.valueOverpricedMax > 0 ? t.valueOverpricedMax : 0.0;
  const needRpdU = spec.undervaluedOnly && t.rpdUndervaluedMin > 0 ? 1 : 0;
  const needRpdO = spec.overpricedOnly && t.rpdOverpricedMax > 0 ? 1 : 0;

  const catNeedles = (spec.categoryContains || []).map((s) => String(s).toLowerCase());
  const mechNeedles = (spec.mechanismContains || []).map((s) => String(s).toLowerCase());
  const kwParam =
    spec.keyword == null || String(spec.keyword).trim() === "" ? null : String(spec.keyword).trim();

  const bggAllow =
    spec.bggIdAllowList && spec.bggIdAllowList.length
      ? spec.bggIdAllowList.map((x) => String(x))
      : [];

  const params = {
    bggAllow: bggAllow,
    kw: kwParam,
    players: spec.players != null ? neo4jRef.int(spec.players) : null,
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
    undF: spec.undervaluedOnly ? neo4jRef.int(1) : neo4jRef.int(0),
    ovF: spec.overpricedOnly ? neo4jRef.int(1) : neo4jRef.int(0),
    tUndS: tUndS,
    tOvrS: tOvrS,
    needRpdU: neo4jRef.int(needRpdU),
    rpdUmin: t.rpdUndervaluedMin,
    needRpdO: neo4jRef.int(needRpdO),
    rpdOmax: t.rpdOverpricedMax,
    wDropF: spec.minPriceDrop != null && spec.minPriceDrop > 0 ? neo4jRef.int(1) : neo4jRef.int(0),
    minPdrop: minPdrop,
    winDays: neo4jRef.int(windowDays),
    cap: neo4jRef.int(capN)
  };

  traceStep("search", "3. cypher params ready", {
    hasKeyword: params.kw != null,
    bggAllowCount: bggAllow.length,
    catNeedles: params.catNeedles.length,
    mechNeedles: params.mechNeedles.length,
    candidateCap: capN,
    winDays: windowDays,
    thresholds: { tUndS, tOvrS, rpdUmin: t.rpdUndervaluedMin, rpdOmax: t.rpdOverpricedMax, needRpdU, needRpdO }
  });
  const t0 = Date.now();
  const result = await runQuery(CYPHER_SEARCH, params, "CYPHER_SEARCH");
  traceStep("search", "4. cypher returned", { ms: Date.now() - t0, rawRecordCount: result.records.length });
  const allHits = mapSearchRecords(result, spec);
  traceStep("search", "5. after sort+map", { sortedCount: allHits.length, returnLimit: finalLimit });
  const sliced = allHits.slice(0, finalLimit);
  traceStep("search", "6. runSearchQuery end", { hitsReturned: sliced.length, top: sliced[0]?.game?.name ?? null });
  return { hits: sliced, spec };
}
