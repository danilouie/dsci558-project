/**
 * HTTP client for the local FAISS embedding service (Python).
 */

const FAISS_SERVICE_URL = (process.env.FAISS_SERVICE_URL || "http://127.0.0.1:5100").replace(/\/$/, "");
const FAISS_TIMEOUT_MS = Math.max(3000, Number(process.env.FAISS_TIMEOUT_MS || 120000));

/**
 * @param {string} path
 * @param {object} body
 */
async function postJson(path, body) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), FAISS_TIMEOUT_MS);
  try {
    const res = await fetch(`${FAISS_SERVICE_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`FAISS service HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    return /** @type {Promise<any>} */ (res.json());
  } finally {
    clearTimeout(t);
  }
}

/** @type {string[] | null} */
let categorySlugsCache = null;

/**
 * Category shard names for NL routing (cached in process).
 * @returns {Promise<string[]>}
 */
export async function faissListCategorySlugs() {
  if (categorySlugsCache && categorySlugsCache.length) {
    return categorySlugsCache;
  }
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), FAISS_TIMEOUT_MS);
  try {
    const res = await fetch(`${FAISS_SERVICE_URL}/v1/category-slugs`, {
      signal: controller.signal
    });
    if (!res.ok) {
      const text = await res.text();
      throw new Error(`FAISS category-slugs HTTP ${res.status}: ${text.slice(0, 200)}`);
    }
    const data = await res.json();
    const slugs = Array.isArray(data?.slugs) ? data.slugs.map((x) => String(x)) : [];
    categorySlugsCache = slugs;
    return slugs;
  } finally {
    clearTimeout(t);
  }
}

/**
 * @param {string} phrase
 * @param {number} [topK]
 */
export async function faissResolveName(phrase, topK = 5) {
  return postJson("/v1/resolve-name", { phrase: String(phrase || "").trim(), top_k: topK });
}

/**
 * @param {object} p
 * @param {string} p.text - description text to embed
 * @param {number} [p.top_k]
 * @param {string | null} [p.exclude_bgg_id]
 * @param {string | null} [p.category_slug] - description index under cat/<slug>/ (omit or null for all_games)
 */
export async function faissSimilarByDescription(p) {
  const body = {
    text: p.text,
    top_k: p.top_k ?? 50,
    exclude_bgg_id: p.exclude_bgg_id != null ? String(p.exclude_bgg_id) : null,
    category_slug:
      p.category_slug != null && String(p.category_slug).trim() !== ""
        ? String(p.category_slug).trim()
        : null
  };
  return postJson("/v1/similar-by-description", body);
}
