# Runbook: board game app (Neo4j + Node + Vite + Ollama + FAISS)

This document explains how to install, configure, and run the full stack. The main app code lives under [`app/`](app/).

**Natural-language search** (`USE_OLLAMA_NL=true`): the backend routes prompts with **Ollama** into **filtering**, **similar_to_game**, or **both** (see [NL pipeline](#nl-pipeline-filtering-vs-similarity) below).

**“Similar to &lt;game&gt;”** with `USE_FAISS_SIMILAR=true`: name index → Neo4j description → **description** FAISS. The service uses the global [`all_games`](artifacts/game_description_by_category_minilm/all_games) index by default, or a **per-category** index under [`cat/<slug>/`](artifacts/game_description_by_category_minilm/cat) when the prompt scopes similarity to a category (Ollama picks a **valid shard slug** from `GET /v1/category-slugs`, with a regex/needle fallback from `categoryContains`). Results are a `bgg_id` allowlist **intersected** in Neo4j with all other filters; **Neo4j `categoryContains` still applies** as a safety net when the user names a category in text.

---

## What you need installed

| Requirement | Purpose |
|-------------|---------|
| **Node.js 18+** and **npm** | Frontend + backend |
| **Neo4j** (Desktop, Docker, or tarball) | Graph database (`:Game`, edges, prices, etc.) |
| **Ollama** | LLM JSON parsing (`USE_OLLAMA_NL=true`) |
| **Python 3.10+** (3.11/3.12 recommended) | FAISS embedding service (`USE_FAISS_SIMILAR=true`) |

---

## Repository layout (important paths)

```
dsci558-project/
├── app/                         # npm workspaces: backend + frontend
│   ├── backend/                 # Express API (Neo4j, Ollama client, FAISS HTTP client)
│   └── frontend/               # React + Vite UI
├── bgg_id_name/                # FAISS index + id_map for **game-name** matching
├── artifacts/game_description_by_category_minilm/all_games/
│                               # FAISS index + id_map for **description similarity**
├── artifacts/game_description_by_category_minilm/cat/<slug>/  # Per-category description shards (lazy-loaded LRU when used)
└── faiss_service/               # Python FastAPI: **bgg_id_name** + **all_games** always; **cat/&lt;slug&gt;** on demand
```

If you move the repo or indexes, set `DSC_PROJECT_ROOT` / `BGG_ID_NAME_DIR` / `ALL_GAMES_DIR` / `CATEGORY_SHARDS_DIR` (see [FAISS service](#4-python-faiss-service-optional-but-required-for-similar-to-game)).

---

## NL pipeline: filtering vs similarity

When `USE_OLLAMA_NL=true`, [`parseNaturalLanguageMessage`](app/backend/src/nlQuery.js) runs:

1. **`classifyPromptKind`** — `filtering` | `similar_to_game` | `both`.
2. **Filtering-only** (`filtering`): two-step Ollama filter extraction → regex fallbacks (`mergeRatingPriceHints`, `mergeQuotedCategoryMechanismHints`) → **return** (no FAISS).
3. **Similarity** (`similar_to_game` or `both`): same filter extraction when needed (`both`, or “similar + filters” heuristics) → merge regex hints → **not** early-return.
4. **Category shard for vectors** (if `USE_FAISS_SIMILAR`): `GET /v1/category-slugs` → **`selectFaissCategorySlug`** (Ollama) → optional **`matchCategorySlugFromNeedles`** fallback → sets `nlParse.faissCategorySlug` / `faissIndex`.
5. **Anchor**: `resolve-name` → Neo4j description → **`POST /v1/similar-by-description`** with optional `category_slug` (retries without slug if the shard errors).
6. Neo4j search merges **`bggIdAllowList`** with all **`QuerySpec`** filters.

Environment: **`CATEGORY_FAISS_CACHE_MAX`** (default `4`) caps how many category indexes stay open in RAM on the Python side.

---

## 1. Neo4j

1. Start Neo4j so Bolt is reachable (default local: `bolt://localhost:7687`).
2. Import or restore your dataset so queries against `(:Game)` work.

You will point the backend at this instance with `NEO4J_*` variables (see below).

---

## 2. Node dependencies (backend + frontend)

From the **`app`** directory:

```bash
cd app
npm install
```

This installs workspaces `backend`, `frontend`, and root dev tools (`concurrently`).

---

## 3. Environment variables

### 3.1 Backend — `app/backend/.env`

Create or edit **`app/backend/.env`**. Minimal example:

```env
PORT=4000

# Must match the browser origin of the Vite dev server (scheme + host + port).
FRONTEND_ORIGIN=http://localhost:5173

NEO4J_URI=bolt://localhost:7687
NEO4J_DATABASE=neo4j
NEO4J_AUTH_MODE=basic
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password_here
```

**CORS:** If you change Vite’s port (e.g. `5183`), set `FRONTEND_ORIGIN=http://localhost:5183`. A mismatch causes browser CORS errors.

**Optional logging:**

```env
TRACE_STEPS=false
```

#### Natural language + similarity (Ollama + FAISS)

Append when you want the full NL pipeline ([`app/backend/env.example`](app/backend/env.example)):

```env
USE_OLLAMA_NL=true
USE_FAISS_SIMILAR=true

OLLAMA_HOST=http://127.0.0.1:11434
OLLAMA_MODEL=llama3
OLLAMA_TIMEOUT_MS=60000

FAISS_SERVICE_URL=http://127.0.0.1:5100
FAISS_TIMEOUT_MS=120000
FAISS_TOP_K=80
```

To run **without** Ollama/FAISS (heuristic message parsing only):

```env
USE_OLLAMA_NL=false
USE_FAISS_SIMILAR=false
```

### 3.2 Frontend — `app/frontend/.env`

Create **`app/frontend/.env`**:

```env
VITE_API_URL=http://localhost:4000
```

Use the same host/port as `PORT` in the backend (`4000` unless you changed it).

---

## 4. Python FAISS service (optional but required for “similar to &lt;game&gt;”)

When `USE_FAISS_SIMILAR=true`, the backend calls this HTTP API to:

- resolve an anchor phrase (e.g. “Catan”) → `bgg_id` using **`bgg_id_name`**
- load that game’s **description** from Neo4j, embed it, and search a **description** index:
  - **`all_games`** when `category_slug` is omitted (default), or
  - **`cat/<slug>/`** when the NL layer passes **`category_slug`** (lazy-loaded LRU cache; list slugs with **`GET /v1/category-slugs`**).

Category phrases still populate **`categoryContains`** for Neo4j substring matching **in addition** to optional category-scoped FAISS (intersect for precision).

### 4.1 Create a virtualenv and install dependencies

From the **repository root** (`dsci558-project`), not inside `app/`:

```bash
cd /path/to/dsci558-project
python3 -m venv .venv-faiss
```

Activate:

- **macOS / Linux:**  
  `source .venv-faiss/bin/activate`
- **Windows (cmd):**  
  `.venv-faiss\Scripts\activate.bat`
- **Windows (PowerShell):**  
  `.venv-faiss\Scripts\Activate.ps1`

Install:

```bash
pip install --upgrade pip
pip install -r faiss_service/requirements.txt
```

The first run will download **`sentence-transformers/all-MiniLM-L6-v2`** (large download).

### 4.2 Run the API server

Still from repo root with the venv activated:

```bash
python faiss_service/server.py
```

Defaults:

- Listens on **`http://127.0.0.1:5100`**
- Loads indexes from `./bgg_id_name` and `./artifacts/game_description_by_category_minilm/all_games/` relative to repo root unless overridden.

Optional environment variables **for the Python process**:

| Variable | Meaning |
|----------|---------|
| `PORT` | Listen port (default `5100`) |
| `DSC_PROJECT_ROOT` | Absolute path to repo root if defaults fail |
| `BGG_ID_NAME_DIR` | Override directory for name index |
| `ALL_GAMES_DIR` | Override directory for description index |
| `SENTENCE_TRANSFORMER_MODEL` | Override model id (must match index build) |
| `CATEGORY_SHARDS_DIR` | Optional: path to per-category shard parent (default: sibling of `all_games`, i.e. `.../game_description_by_category_minilm/cat`) |
| `CATEGORY_FAISS_CACHE_MAX` | Max category indexes kept open in RAM (default `4`; LRU eviction) |

List valid category slugs for NL routing:

```bash
curl -s http://127.0.0.1:5100/v1/category-slugs
```

Health check:

```bash
curl -s http://127.0.0.1:5100/health
```

The JSON includes **`name_vectors`** / **`games_vectors`**, **`category_shards_*`**, **`category_shards_lazy_cache`**, **`category_faiss_cache_max`**, and **`category_shards_loaded_in_memory`** (number of category indexes currently cached, not total shard folders).

Keep this terminal open (or run under `tmux`/systemd).

---

## 5. Ollama (optional but required when `USE_OLLAMA_NL=true`)

Ensure Ollama is running (`ollama serve` if needed). Pull the model matching `OLLAMA_MODEL`:

```bash
ollama pull llama3
```

Verify:

```bash
curl -s http://127.0.0.1:11434/api/tags
```

---

## 6. Run the web app

### 6.1 Backend + frontend together

From **`app`**:

```bash
cd app
npm run dev
```

This starts:

- **Backend:** `http://localhost:4000` (unless `PORT` differs)
- **Frontend:** typically `http://localhost:5173` (Vite default)

Open the frontend URL in the browser.

### 6.2 Run backend and frontend in separate terminals

From **`app`**:

```bash
npm run dev:backend
```

```bash
npm run dev:frontend
```

### 6.3 Production build (frontend only)

From **`app`**:

```bash
npm run build
```

Outputs static files under `app/frontend/dist/` (serve with any static host; point API to your deployed backend).

---

## 7. Verification checklist

```bash
# Backend up + Neo4j reachable
curl -s http://localhost:4000/api/health

# FAISS service (if using similarity)
curl -s http://127.0.0.1:5100/health

# Ollama (if using NL)
curl -s http://127.0.0.1:11434/api/tags
```

---

## 8. Typical startup order

1. **Neo4j** running  
2. **FAISS Python service** (if `USE_FAISS_SIMILAR=true`)  
3. **Ollama** with model pulled (if `USE_OLLAMA_NL=true`)  
4. **`cd app && npm run dev`**

---

## 9. Troubleshooting

| Problem | What to check |
|---------|----------------|
| Browser CORS errors | `FRONTEND_ORIGIN` in `app/backend/.env` matches the URL bar (including port). |
| Frontend cannot reach API | `VITE_API_URL` in `app/frontend/.env`; backend running; restart Vite after changing `.env`. |
| Backend Neo4j errors | `NEO4J_URI`, credentials, DB name; Neo4j actually listening on Bolt. |
| NL parsing fails / timeouts | Ollama running; `OLLAMA_MODEL` pulled; raise `OLLAMA_TIMEOUT_MS`. |
| “Similar to game” empty or errors | FAISS service running; `FAISS_SERVICE_URL`; indexes exist under expected paths; Neo4j `Game` has `bgg_id` / `description` for anchor. |
| Similar + **category** returns nothing | Category needle must **substring-match** BGG category strings on `Game.categories` (try shorter tokens: `strategy`, `economic`, `wargame`). Check `TRACE_STEPS`/logs for merged `QuerySpec`. |
| Heuristic-only mode | Set `USE_OLLAMA_NL=false` and `USE_FAISS_SIMILAR=false`; no Ollama/FAISS needed. |

---

## 10. Quick reference: environment files

| File | Role |
|------|------|
| [`app/backend/.env`](app/backend/.env) | DB, CORS, Ollama, FAISS client settings |
| [`app/frontend/.env`](app/frontend/.env) | `VITE_API_URL` → backend base URL |
| [`app/backend/env.example`](app/backend/env.example) | Template for NL + FAISS flags |

---

## 11. Related docs

- [`app/README.md`](app/README.md) — shorter app-focused README  
- [`usecases.md`](usecases.md) — API / product use cases  
- [`SCHEMA.md`](SCHEMA.md) — Neo4j property names (`bgg_id`, `description`, etc.)

---

## 12. NL pipeline summary (for operators)

| Step | Component | Role |
|------|-----------|------|
| 1 | Ollama | Classifies **filtering** / **similar_to_game** / **both**; extracts **`similarToGame`** when relevant. |
| 2 | Ollama | **Filtering-only**: filter steps → Neo4j (no FAISS). **Similarity**: step A/B as needed for **`both`** or similar+filters; regex fallbacks for price/rating and quoted categories. |
| 3 | Node + Ollama | **Similarity only**: optional **`faissCategorySlug`** from prompt vs `GET /v1/category-slugs`, else needle match on **`categoryContains`**. |
| 4 | FAISS (optional) | Resolve anchor name → `bgg_id`; embed Neo4j **description**; top‑K **`bgg_id`** allowlist from **`all_games`** or **`cat/<slug>/`** (`category_slug` body field). |
| 5 | Neo4j | `runSearchQuery`: merges keyword, **`bgg_id` IN allowlist** (when set), **`categoryContains`** needles on lists, presets, sorts, prices. |

Heuristic fallback if Ollama fails: legacy regex heuristics in [`app/backend/src/searchQuery.js`](app/backend/src/searchQuery.js) (`messageToQuerySpec`).

---

## 13. Example prompts (use cases)

Paste these into the app **Chat / message** field for **Recommend** or **Search** (with **Merge message** enabled for search).  
**Requirements:** `USE_OLLAMA_NL=true`. For similarity rows below, also **`USE_FAISS_SIMILAR=true`** + FAISS service running.

### A. Filtering only (budget, players, presets — no anchor game)

- `Games under $35 with at least 7 Geek rating`
- `High want low own games`
- `Undervalued strategy games`
- `2 players, under 90 minutes`

### B. Similar to one game (global description similarity)

- `Games similar to Catan`
- `Something like Wingspan`
- `Recommend titles similar to Terraforming Mars`

### C. Similar to a game **and** constrained to a **category**

Use wording the model and regex helpers recognize (`category`, quoted game-style categories, etc.):

- `Similar games to Catan in the strategy category`
- `Games like Gloomhaven in "fantasy" category`
- `Similar to Wingspan in this "economic" category`
- `Like Ticket to Ride in the family category`

If results are empty, shorten the category token to match how BGG stores it (often lowercase substrings like `strategy`, `card game`, `economic`).

### D. Combined (“both”): similarity + extra filters

- `High want low own games similar to Catan`
- `Under $40, games like Azul`

### E. Mechanism-oriented (still Neo4j list match, not a separate FAISS index)

- `Worker placement games like Agricola`
- `Games like Blood Rage with deck-building mechanism`

---

**Tip:** Responses may include **`nlParse`** JSON (prompt kind, anchor **`bgg_id`**, whether FAISS narrowed the set) from **`POST /api/search`** and **`POST /api/recommend`** — useful when debugging prompts.
