# Natural language → query: types, options, and one example each

This document lists **all selection options** used by the Ollama NL pipeline (`ollamaNlp.js` + `nlQuery.js`) and the shared **`QuerySpec`** / **`QueryPresetId`** / **`SearchSortField`** types. For each type, we give **one user message example** and the **intended machine interpretation** (JSON shape or field value).

Use it to tune prompts, write tests, or debug traces.

---

## 1. Step 1 — `classifyPromptKind` (`PromptKind`)

**Output shape (only valid JSON, no markdown):**

```json
{ "promptKind": "similar_to_game" | "filtering" | "both", "similarToGame": string | null }
```

| `promptKind`    | When to use | `similarToGame` | User example (one) | Model output (example) |
|-----------------|------------|-----------------|--------------------|------------------------|
| **`similar_to_game`** | User wants games *like* a **named title only** — no budget, player count, category, mechanism, or preset beyond that. | **Non-null** — the anchor title. | *"Find me games like Brass Birmingham"* | `{"promptKind":"similar_to_game","similarToGame":"Brass Birmingham"}` |
| **`filtering`**     | **Only** constraints: price, players, time, category/mechanism substrings, presets, sorts, “undervalued”, “high want low own”, etc. **No** reference game. | `null` | *"co-op games for 3 players under 2 hours, under 40 dollars"* | `{"promptKind":"filtering","similarToGame":null}` |
| **`both`**          | A **named reference game** **and** at least one extra constraint (price, players, time, **quoted category**, mechanism, preset, etc.). | **Non-null** — the anchor title. | *"Games like Viticulture but cheaper than 35 dollars for 2 players"* | `{"promptKind":"both","similarToGame":"Viticulture"}` |

**Disambiguation:**

- *"Wingspan in the economic category"* with **category** in the same sentence as “like W…” → usually **`both`** (anchor + `categoryContains`).
- *"high want low own games"* (no game name) → **`filtering`**; downstream steps should set **`preset`** to `high_want_low_own`, not confuse with `undervaluedOnly`.

---

## 2. Query presets (`QueryPresetId` → `preset` in `QuerySpec`)

Each id is the **exact string** the model must use in JSON. **One user-style example** per preset:

| `preset` id | What it means in this app (summary) | One user message example | Intended `preset` in JSON |
|------------|-------------------------------------|--------------------------|-----------------------------|
| `best_under_budget` | Tight default budget + decent rating, sort by price (see `searchQuery` PRESET_DEFAULTS). | *"best games under 30 dollars"* | `"best_under_budget"` |
| `value_for_price` | Emphasize rating per dollar. | *"I want the best value for my money, rating per dollar"* | `"value_for_price"` |
| `highly_rated_cheap` | High min rating, low price cap. | *"highly rated but cheap, under 30 dollars"* | `"highly_rated_cheap"` |
| `overpriced` | “Overpriced” / bad value signals. | *"show me overpriced games"* | `"overpriced"` |
| `undervalued` | Model / heuristics for “undervalued” buys. | *"undervalued games, good value score"* | `"undervalued"` |
| **`high_want_low_own`** | **Wants** high vs **owns** low — sort by `want_minus_own`. **Not** the same as `undervalued`. | *"high want low own games"* | `"high_want_low_own"` |
| `frequently_traded` | Emphasize want-to-trade style signal. | *"games that are traded a lot, frequently traded"* | `"frequently_traded"` |
| `rating_per_dollar` | Sort by geek rating / price. | *"sort by rating per dollar"* | `"rating_per_dollar"` |
| `composite_demo` | Demo bundle of caps + min wants + rating (see preset defaults). | *"use the smart demo bundle"* (or UI chip label) | `"composite_demo"` |

---

## 3. Sort fields (`SearchSortField` → `sort` in `QuerySpec`)

Use with optional `sortDirection` (`"asc"` | `"desc"`). **One user phrase** per field:

| `sort` value | Typical user phrasing (one example) | Notes |
|-------------|----------------------------------------|--------|
| `rating` | *"sort by geek rating, highest first"* | Default in merge when unset. |
| `geek_rating` | *"order by BGG geek rating desc"* | Same family as `rating` in the graph. |
| `rank_value_asc` | *"best BGG rank number, low rank at the top"* | Better rank = smaller number. |
| `mean_price` | *"cheapest by latest average price, ascending price"* | Uses latest `:PricePoint` mean. |
| `rating_per_dollar` | *"most rating bang for the buck"* | Heuristic; needs price. |
| **`want_minus_own`** | *"most wanted compared to how many people own it"* | Aligns with **high want low own** style. |
| `wtt_to_wants` | *"high want-to-trade relative to wants"* | Trade-heavy discovery. |
| `value_score` | *"sort by my value_score column on the graph"* | If property missing, ORDER BY may no-op. |
| `wants` | *"most wanted games on BGG, by want count"* | |
| `wtt` | *"most WTT, want to trade"* | |
| `price_drop` | *"biggest price drop from recent window vs latest"* | Uses `priceWindowDays` / `minPriceDrop` when set. |

---

## 4. Filter “active keys” (Step 2 — `filterStepActiveKinds` → `active: string[]`)

The model may only list keys it sees in the user message. Below: **key** and **one user example** that should activate it.

| Key | One user example that should set this key |
|-----|-------------------------------------------|
| `keyword` | *"search for Gloomhaven in the title"* (direct title search, not “similar to X” anchor) |
| `preset` | *"high want low own games"* → also activate **`preset`**, not only `undervaluedOnly` |
| `players` | *"for 4 people"* / *"2 player games"* |
| `maxTime` | *"under 90 minutes"* / *"max 2 hours play time"* |
| `maxPrice` | *"under 50 dollars"*, *"below 40$"* |
| `minPrice` | *"at least 20 dollars"*, *"over 25$"* |
| `minRating` | *"rating 8 or higher"*, *"at least 7.5 BGG"* |
| `categoryContains` | *"in the **strategy** category"*, *"euro / economic family"* |
| `mechanismContains` | *"worker placement"*, *"deck building only"* |
| `minComplexity` | *"weight at least 3"*, *"heavier than 2.5 complexity"* |
| `maxComplexity` | *"not too heavy, under complexity 2"* |
| `minWants` | *"at least 500 people want it on BGG"* (numeric) |
| `minOwns` | *"at least 1000 owns"* (numeric) |
| `undervaluedOnly` | *"truly undervalued", "model says undervalued, good value score"* — **not** the same phrase as “high want low own” if that should map to **`preset: high_want_low_own`**. |
| `overpricedOnly` | *"overpriced, bad value"* |
| `sort` | *"sort by price ascending"* → pair with `sort` + `sortDirection` |
| `sortDirection` | *"cheapest first"* vs *"highest first"* |
| `usernameExcludesOwns` | *"I don’t own" / exclude user X’s collection* (if username provided) |
| `priceWindowDays` | *"compared to prices in the last 6 months* |
| `minPriceDrop` | *"dropped in price at least 10 dollars from the window min"* |

---

## 5. Optional FAISS — `selectFaissCategorySlug` (when `USE_FAISS_SIMILAR` is on)

**Output (only valid JSON):**

```json
{ "faissCategorySlug": "exact_slug_from_allowlist" | null }
```

Allowlist = dynamic (`GET` from the Python FAISS service; folder names under `cat/`). **One example** each:

| Situation | User example | Typical `faissCategorySlug` |
|----------|--------------|----------------------------|
| **No** category filter for the vector index | *"Games similar to Scythe"* (any genre) | `null` (search **all_games** index) |
| **Yes** — ties similarity to a category | *"wargame like Root"* | One matching slug, e.g. `wargame` **if** present in allowlist |
| **Yes** — another category | *"Economic game like Food Chain Magnate"* | e.g. `economic` if present |
| Unsure or slug not in list | *"weird mix of adjectives, no clear shard"* | `null` |

---

## 6. Pipeline order (Ollama steps, when `USE_OLLAMA_NL=true`)

1. **`1.classifyPromptKind`** — Section 1 above.  
2. **`2.filterStepActiveKinds`** — `active` array from Section 4.  
3. **`3.filterStepExtractValues`** — values only for keys in `active`.  
4. **`4.reconcileQuerySpec`** — merge/correct; must **only** output keys the user actually implied.  
5. **`5.selectFaissCategorySlug`** — only if the route needs similarity and FAISS is enabled — Section 5.  
6. **FAISS resolve + similar-by-description** (not Ollama) — builds `bggIdAllowList` for Neo4j search.  

---

## 7. “Master” reminder block (drop-in for system prompt tuning)

You can append this to internal NL prompts so models keep **`high_want_low_own`** and **`undervalued`** distinct, and return **only JSON**:

```
Routing:
- "similar_to_game" = one named game, no other constraints.
- "filtering" = constraints only (including presets and sorts).
- "both" = named game + at least one constraint (price, time, "strategy" category, etc.).

Presets (exact id strings):
best_under_budget, value_for_price, highly_rated_cheap, overpriced, undervalued,
high_want_low_own, frequently_traded, rating_per_dollar, composite_demo

Critical: Phrases "high want", "low own", "want to own spread", "more wants than owns"
map to preset "high_want_low_own" (or sort "want_minus_own"), NOT to undervaluedOnly alone.

Output: a single JSON object, no markdown fences, no extra commentary.
```

---

*Generated for `dsci558-project` / `app/backend` NL pipeline. Preset *defaults* and Cypher behavior live in `searchQuery.js` (`PRESET_DEFAULTS`, `applyPresetMerge`).*
