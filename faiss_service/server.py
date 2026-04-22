"""
Local FAISS + MiniLM API for game-name resolution and description similarity.
Defaults: repo-root bgg_id_name and all_games artifact dirs.
"""

from __future__ import annotations

import json
import os
from collections import OrderedDict
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer

REPO_ROOT = Path(
    os.environ.get("DSC_PROJECT_ROOT", Path(__file__).resolve().parent.parent)
).resolve()

NAME_DIR = Path(os.environ.get("BGG_ID_NAME_DIR", REPO_ROOT / "bgg_id_name")).resolve()
GAMES_DIR = Path(
    os.environ.get(
        "ALL_GAMES_DIR",
        REPO_ROOT / "artifacts" / "game_description_by_category_minilm" / "all_games",
    )
).resolve()

# Per-category offline shards (metadata only for /health — not loaded into RAM by default).
CATEGORY_SHARDS_DIR = Path(
    os.environ.get(
        "CATEGORY_SHARDS_DIR",
        GAMES_DIR.parent / "cat",
    )
).resolve()

MODEL_NAME = os.environ.get(
    "SENTENCE_TRANSFORMER_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
)

# LRU: lazy-loaded category description indexes (slug -> (faiss.Index, DataFrame indexed by faiss_id))
CATEGORY_FAISS_CACHE_MAX = max(1, int(os.environ.get("CATEGORY_FAISS_CACHE_MAX", "4")))
_category_index_cache: OrderedDict[str, tuple] = OrderedDict()

_valid_category_slugs: frozenset[str] | None = None

model = SentenceTransformer(MODEL_NAME)
name_index = faiss.read_index(str(NAME_DIR / "index.faiss"))
games_index = faiss.read_index(str(GAMES_DIR / "index.faiss"))
name_map = pd.read_parquet(NAME_DIR / "id_map.parquet")
games_map = pd.read_parquet(GAMES_DIR / "id_map.parquet")

name_by_faiss = name_map.set_index("faiss_id")
games_by_faiss = games_map.set_index("faiss_id")


def scan_category_shard_dirs(cat_parent: Path) -> dict:
    """
    List per-category artifact folders (each may contain index.faiss + meta.json).
    Used for visibility on /health only; indexes are not opened unless you add a future API.
    """
    out: dict = {
        "category_shards_dir": str(cat_parent),
        "category_shard_count": 0,
        "category_shards_total_vectors": 0,
        "category_shards": [],
    }
    if not cat_parent.is_dir():
        return out

    shards: list[dict] = []
    total_vecs = 0
    for d in sorted(cat_parent.iterdir()):
        if not d.is_dir():
            continue
        idx_file = d / "index.faiss"
        if not idx_file.exists():
            continue
        meta_path = d / "meta.json"
        num_vectors = None
        model_from_meta = None
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                num_vectors = meta.get("num_vectors")
                model_from_meta = meta.get("model_name")
            except (OSError, json.JSONDecodeError):
                pass
        rec = {
            "slug": d.name,
            "dir": str(d),
            "num_vectors": num_vectors,
        }
        if model_from_meta:
            rec["model_name"] = model_from_meta
        shards.append(rec)
        if isinstance(num_vectors, int):
            total_vecs += num_vectors

    out["category_shard_count"] = len(shards)
    out["category_shards_total_vectors"] = total_vecs if shards else 0
    out["category_shards"] = shards
    return out


def bgg_from_idx(idx_df: pd.DataFrame, ix: int) -> str | None:
    """Look up bgg_id for FAISS row index."""
    try:
        row = idx_df.loc[int(ix)]
        return str(row["bgg_id"])
    except (KeyError, TypeError, ValueError):
        return None


def refresh_valid_category_slugs() -> frozenset[str]:
    """Slug names under CATEGORY_SHARDS_DIR that have index.faiss."""
    global _valid_category_slugs
    info = scan_category_shard_dirs(CATEGORY_SHARDS_DIR)
    slugs = {s["slug"] for s in info.get("category_shards", []) if s.get("slug")}
    _valid_category_slugs = frozenset(slugs)
    return _valid_category_slugs


def get_valid_category_slugs() -> frozenset[str]:
    global _valid_category_slugs
    if _valid_category_slugs is None:
        refresh_valid_category_slugs()
    return _valid_category_slugs


def load_category_shard(slug: str) -> tuple:
    """Return (faiss.Index, id_map indexed by faiss_id); LRU-cached."""
    if slug in _category_index_cache:
        _category_index_cache.move_to_end(slug)
        return _category_index_cache[slug]

    cat_dir = CATEGORY_SHARDS_DIR / slug
    idx_path = cat_dir / "index.faiss"
    map_path = cat_dir / "id_map.parquet"
    if not idx_path.is_file() or not map_path.is_file():
        raise FileNotFoundError(f"Missing shard files under {cat_dir}")

    idx = faiss.read_index(str(idx_path))
    m = pd.read_parquet(map_path)
    by_faiss = m.set_index("faiss_id")
    tup = (idx, by_faiss)

    _category_index_cache[slug] = tup
    _category_index_cache.move_to_end(slug)
    while len(_category_index_cache) > CATEGORY_FAISS_CACHE_MAX:
        _category_index_cache.popitem(last=False)

    return tup


app = FastAPI(title="Board game FAISS service")


@app.get("/health")
def health():
    cat_info = scan_category_shard_dirs(CATEGORY_SHARDS_DIR)
    payload = {
        "ok": True,
        "model": MODEL_NAME,
        "name_vectors": int(name_index.ntotal),
        "games_vectors": int(games_index.ntotal),
        "name_dir": str(NAME_DIR),
        "games_dir": str(GAMES_DIR),
    }
    payload.update(cat_info)
    payload["category_shards_lazy_cache"] = True
    payload["category_faiss_cache_max"] = CATEGORY_FAISS_CACHE_MAX
    payload["category_shards_loaded_in_memory"] = len(_category_index_cache)
    if not CATEGORY_SHARDS_DIR.is_dir():
        payload["category_shards_note"] = (
            f"No category shards directory at {CATEGORY_SHARDS_DIR}; set CATEGORY_SHARDS_DIR if artifacts live elsewhere."
        )
    else:
        payload["category_shards_note"] = (
            "Per-category description indexes under cat/<slug>/ are loaded on demand into an LRU cache "
            f"(max {CATEGORY_FAISS_CACHE_MAX}). POST /v1/similar-by-description with category_slug uses that shard; "
            "omit category_slug for all_games. Slugs: GET /v1/category-slugs."
        )
    return payload


@app.get("/v1/category-slugs")
def category_slugs():
    """Finite list of category shard directory names (for NL routing / Ollama allowlist)."""
    refresh_valid_category_slugs()
    slugs = sorted(get_valid_category_slugs())
    return {"slugs": slugs}


class ResolveNameRequest(BaseModel):
    phrase: str = ""
    top_k: int = Field(default=5, ge=1, le=50)


@app.post("/v1/resolve-name")
def resolve_name(req: ResolveNameRequest):
    phrase = req.phrase.strip()
    if not phrase:
        return {"results": []}

    emb = model.encode([phrase], normalize_embeddings=True)
    k = min(req.top_k, int(name_index.ntotal))
    scores, indices = name_index.search(np.asarray(emb, dtype=np.float32), k)

    results = []
    for ix, score in zip(indices[0].tolist(), scores[0].tolist()):
        bid = bgg_from_idx(name_by_faiss, ix)
        if bid is None:
            continue
        results.append({"faiss_id": int(ix), "bgg_id": bid, "score": float(score)})
    return {"results": results}


class SimilarDescRequest(BaseModel):
    text: str = ""
    top_k: int = Field(default=50, ge=1, le=500)
    exclude_bgg_id: str | None = None
    """When set, search this category's description index instead of all_games."""
    category_slug: str | None = None


@app.post("/v1/similar-by-description")
def similar_by_description(req: SimilarDescRequest):
    text = req.text.strip()
    if not text:
        return {"bgg_ids": [], "index_used": "none", "category_slug": None}

    raw_slug = req.category_slug.strip() if req.category_slug else ""
    idx_df_by_faiss = games_by_faiss
    idx = games_index
    index_used = "all_games"
    active_slug: str | None = None

    if raw_slug:
        valid = get_valid_category_slugs()
        if raw_slug not in valid:
            raise HTTPException(
                status_code=422,
                detail=f"Unknown category_slug {raw_slug!r}. Use GET /v1/category-slugs for allowed values.",
            )
        try:
            idx, idx_df_by_faiss = load_category_shard(raw_slug)
        except FileNotFoundError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        index_used = "category"
        active_slug = raw_slug

    emb = model.encode([text], normalize_embeddings=True)
    exclude = req.exclude_bgg_id.strip() if req.exclude_bgg_id else None
    scan_k = min(req.top_k + (50 if exclude else 0), int(idx.ntotal))
    scores, indices = idx.search(np.asarray(emb, dtype=np.float32), scan_k)

    out: list[str] = []
    for ix in indices[0].tolist():
        bid = bgg_from_idx(idx_df_by_faiss, ix)
        if bid is None:
            continue
        if exclude and bid == exclude:
            continue
        out.append(bid)
        if len(out) >= req.top_k:
            break

    return {
        "bgg_ids": out,
        "index_used": index_used,
        "category_slug": active_slug,
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "5100"))
    uvicorn.run(app, host="127.0.0.1", port=port)
