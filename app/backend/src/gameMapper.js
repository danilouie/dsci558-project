/**
 * Map Neo4j :Game node → API GameSummary (BGG-shaped fields from graph ETL).
 * @typedef {import("../../shared/contracts.d.ts").GameSummary} GameSummary
 */

/**
 * @param {unknown} v
 * @param {number | null} [fallback]
 */
function toNum(v, fallback = null) {
  if (v == null) return fallback;
  if (typeof v === "object" && v !== null && "toNumber" in v) {
    try {
      const x = Number(/** @type {{ toNumber: () => number }} */ (v).toNumber());
      return Number.isFinite(x) ? x : fallback;
    } catch {
      return fallback;
    }
  }
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * @param {unknown} v
 */
function neoStringList(v) {
  if (v == null) return [];
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  if (typeof v === "string") return v.split("|").map((s) => s.trim()).filter(Boolean);
  return [];
}

/**
 * BGG descriptions are often HTML; strip tags for safe plain-text UI.
 * @param {unknown} html
 */
function htmlDescriptionToPlain(html) {
  if (html == null || typeof html !== "string") return null;
  const t = html
    .replace(/\r/g, "")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<\/div>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return t.length ? t : null;
}

/**
 * @param {unknown} v
 * @returns {boolean | null}
 */
function toBool(v) {
  if (v === true || v === false) return v;
  if (v == null) return null;
  const s = String(v).toLowerCase();
  if (s === "true") return true;
  if (s === "false") return false;
  return null;
}

/**
 * @param {import("neo4j-driver").Node} node
 * @param {{ estimatedPrice?: number | null }} [extras]
 * @returns {import("../../shared/contracts.d.ts").GameSummary}
 */
export function gameNodeToSummary(node, extras = {}) {
  const p = node.properties;
  const geek = toNum(p.geek_rating ?? p.bayesavg);
  const avg = toNum(p.average);
  const rating = geek != null ? geek : avg;

  const minPt = toNum(p.min_playtime ?? p.minplaytime);
  const maxPt = toNum(p.max_playtime ?? p.maxplaytime);
  const singlePt = toNum(p.playingtime ?? p.maxplaytime ?? p.play_time);

  let playTime = singlePt;
  if (playTime == null && minPt != null && maxPt != null) {
    playTime = Math.round((minPt + maxPt) / 2);
  }

  const ur = toNum(p.usersrated ?? p.num_voters, 0);

  return {
    id: node.elementId,
    bggId: p.bgg_id == null ? null : String(p.bgg_id),
    name: p.name == null ? "Unknown Game" : String(p.name),
    yearPublished: toNum(p.yearpublished ?? p.year_published),
    minPlayers: toNum(p.minplayers ?? p.min_players),
    maxPlayers: toNum(p.maxplayers ?? p.max_players),
    playTime,
    rating: rating ?? 0,
    usersRated: ur ?? 0,
    complexity: toNum(p.complexity),
    geekRating: geek,
    averageRating: avg,
    numVoters: toNum(p.num_voters ?? p.usersrated),
    description: htmlDescriptionToPlain(p.description),
    categories: neoStringList(p.categories),
    mechanisms: neoStringList(p.mechanisms),
    minAge: toNum(p.minage ?? p.min_age),
    minPlaytime: minPt,
    maxPlaytime: maxPt,
    bestMinPlayers: toNum(p.best_min_players ?? p.best_minplayers),
    bestMaxPlayers: toNum(p.best_max_players ?? p.best_maxplayers),
    isExpansion: toBool(p.is_expansion),
    rank: toNum(p.rank),
    estimatedPrice: extras.estimatedPrice != null && Number.isFinite(Number(extras.estimatedPrice)) ? Number(extras.estimatedPrice) : null
  };
}
