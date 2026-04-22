# Use cases: board game value & search

This document describes who uses the app, what they are trying to do, and how the current implementation (filters, presets, `/api/search`, `/api/recommend`) supports each case. It complements [app/README.md](app/README.md) and the [Neo4j schema](SCHEMA.md).

## Actors

- **Casual buyer** — wants good games under a budget with minimal effort.
- **Value-focused shopper** — compares rating, price, and crowd demand before buying.
- **Hobbyist / collector** — explores “hidden gems,” trade behavior, and demand signals in the knowledge graph.
- **Demo audience** (course / presentation) — needs a clear, repeatable story: filter → ranked results → graph neighborhood.

## Preconditions (all cases)

- Neo4j is running and populated with `(:Game)`, `(:PricePoint)` (ideally for overlap games), and optional `(:User)` collection edges (`OWNS`, `WANTS`, `WANTS_TO_BUY`, `WANTS_TO_TRADE`).
- Backend env points at the correct database; frontend `VITE_API_URL` points at the API.
- Optional: `value_score`, `overpriced`, `undervalued` on `(:Game)` after your ML pipeline; otherwise proxies (rating / price) and env thresholds from [SCHEMA.md](SCHEMA.md) still apply.

---

## UC-1: Find strong games under a fixed budget

**Goal:** “Show me well-rated games I can buy without spending more than $X.”

**Primary actor:** Casual buyer, value-focused shopper.

**How they do it (UI):** Open **Filters** → set **Max price** and optionally **Min rating** → **Recommend from filters**; or choose preset **Best under $30** (or adjust budget in the query for other amounts).

**How they do it (API):** `POST /api/recommend` or `POST /api/search` with `query.maxPrice`, `query.minRating`, and a sort such as `mean_price` (asc) or `rating` (desc).

**Success:** Results favor games with a known latest `mean_price` and ratings; the centered game in the graph is the best match for the current sort, with **search explain** (price, wants/owns, etc.) in the side panel when search metadata is present.

**Failure / limits:** Games without price points are excluded from strict max-price paths; data quality depends on your BGO price import.

---

## UC-2: Maximize “value for money”

**Goal:** “Which games give the most rating (or model value) per dollar?”

**Actor:** Value-focused shopper.

**UI:** Preset **Value for price** (sort: rating per dollar) or set **Sort by** to **Rating / $**; combine with **Max price** if needed.

**API:** `query.sort: "rating_per_dollar"`, `sortDirection: "desc"`.

**Success:** Ordered list and graph center align with high `rating / mean_price` (when price exists). If `value_score` exists on `Game`, you can also sort by `value_score` after ETL.

---

## UC-3: Spot overpriced or undervalued titles

**Goal:** “Which hot games are poor value?” or “What is undervalued right now?”

**Actor:** Hobbyist, value-focused shopper.

**UI:** Presets **Overpriced** or **Undervalued**; ensure env thresholds in the backend are set if you rely on `value_score` or rating-per-dollar proxies (see [SCHEMA.md](SCHEMA.md)).

**API:** `query.overpricedOnly: true` or `query.undervaluedOnly: true` (and optional `query.sort`).

**Success:** Filter branch matches boolean flags on the graph, numeric `value_score` thresholds, or the configured RPD proxy when model fields are missing.

**Limits:** Without `overpriced` / `undervalued` / `value_score` on nodes, behavior depends entirely on env-based proxies; tune `RATING_PER_DOLLAR_*` and `VALUE_SCORE_*` for your demo.

---

## UC-4: Use crowd demand and trade behavior

**Goal:** “What do people want but not own yet?” or “What gets traded a lot?”

**Actor:** Hobbyist, collector.

**UI:** **High want, low own** or **Frequently traded**; optionally add **Max price** in filters.

**API:** Sort `want_minus_own` or `wtt`; `query.minWants` to drop low-signal games.

**Success:** Explain block shows WANTS, OWNS, WTB, WTT; ordering reflects aggregate relationships in Neo4j.

**Limits:** Large graphs may need pre-aggregated counts on `Game` if query latency is high (see ETL note in [SCHEMA.md](SCHEMA.md)).

---

## UC-5: Natural-language shortcut (chat-style prompt)

**Goal:** “Strategy games for 3 players under $40” without filling every field.

**Actor:** Any; useful in demos.

**UI:** **Chat** → type a short prompt (price cap, “undervalued,” “2 players,” “90 minutes”).

**API:** The backend merges heuristics from the message with explicit `filters` / `query` in `bodyToQuerySpec` / `mergeQuerySpec` (see [app/backend/src/searchQuery.js](app/backend/src/searchQuery.js)).

**Success:** Heuristics set `maxPrice`, `players`, `maxTime`, `minWants`, category hints, and over/undervalued flags where phrases match.

**Limits:** Heuristics are rule-based, not a full LLM; ambiguous sentences may need manual filter adjustment.

---

## UC-6: List-only search (no graph) or list + graph

**Goal:** Get a ranked table for analysis or export; optionally still open the graph on the top result.

**Actor:** Power user, demo.

**API:** `POST /api/search` with `includeGraph: false` (list only) or `includeGraph: true` (includes `graph` with center = first hit).

**Success:** `hits[]` with `game` and `explain` for each row.

---

## UC-7: “Smart” multi-constraint demo (composite)

**Goal:** “Cheap, highly rated, in demand, and rarely traded” in one query.

**Actor:** Demo audience.

**UI:** Preset **Smart demo** (defaults include max price, minimum wants, minimum rating).

**API:** `preset: "composite_demo"` with optional overrides in `query`.

**Success:** Single request exercises price, rating, and crowd filters together; good for a scripted walkthrough.

---

## UC-8: Explore similar games from a center (graph)

**Goal:** “I picked a game—see related titles and re-center by clicking,” in two different ways depending on how the graph was opened.

**Actor:** Explorer.

**UI:** After a **recommend** or **search (with graph)**, the orbit shows other **query-matched** games from the same ranked list (top K from `runSearchQuery`, same filters and sort as the list). After **load default**, **open by element id**, or **open by BGG id**, the orbit still uses the older **profile similarity** (`getNeighbors` in [app/backend/src/server.js](app/backend/src/server.js)) because there is no active `QuerySpec` for that request.

**API shape:** The graph JSON includes `neighborMode`: `search_hits` (recommend / search) vs `similarity` (browse / drill-in).

**Success (search / recommend path):** Every node in the orbit (except the center) is a hit from the same search list as the current query; order follows the list rank (`#2`, `#3`, … in the UI).

**Success (browse path):** Neighbors are scored with rating, player/time/year overlap, and list overlap on `categories` / `mechanisms`.

**Limits:** The browse path does not apply the same `QuerySpec` as a prior search; it is local similarity from the current center.

---

## Preset quick reference

| Preset ID | Typical intent |
|-----------|----------------|
| `best_under_budget` | Best-rated affordable options, sorted toward low price |
| `value_for_price` | Rating (or value) per dollar |
| `highly_rated_cheap` | High min rating with a cap on price |
| `undervalued` | Model / proxy “good buy” |
| `overpriced` | Model / proxy “poor value for price” |
| `high_want_low_own` | Demand vs ownership |
| `frequently_traded` | High WTT signal |
| `rating_per_dollar` | Same family as value-for-price, explicit RPD focus |
| `composite_demo` | Multi-filter showcase |

---

## Traceability to implementation

- **Structured filters:** [app/shared/contracts.d.ts](app/shared/contracts.d.ts) — `QuerySpec`, `QueryPresetId`, `SearchRequestBody`.
- **Search and presets:** [app/backend/src/searchQuery.js](app/backend/src/searchQuery.js) — Cypher, `messageToQuerySpec`, `applyPresetMerge`.
- **HTTP / graph:** [app/backend/src/server.js](app/backend/src/server.js) — `POST /api/search`, `POST /api/recommend` return a graph built from top search **hits** (`graphFromSearchHits`); `GET` graph routes use `graphFromCenter` + `getNeighbors` when there is no search list.
- **UI:** [app/frontend/src/App.tsx](app/frontend/src/App.tsx) — filter drawer, preset chips, search explain in the side panel.
