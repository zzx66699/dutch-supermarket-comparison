from typing import List, Dict, Any
import requests
import numpy as np
import json

from supabase_utils import get_supabase

HF_SPACE_URL = "https://zzx990907-dutch-supermarket-price-comparison.hf.space/embed"


# -----------------------------
# Call HF Space for embedding
# -----------------------------
def get_embedding_from_hf(text: str) -> np.ndarray:
    payload = {"texts": [text]}
    resp = requests.post(HF_SPACE_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    emb = data["embeddings"][0]
    return np.asarray(emb, dtype="float32")


# -----------------------------
# Parse embedding from database
# -----------------------------
def parse_embedding(emb):
    if emb is None:
        return None

    if isinstance(emb, list):
        return np.asarray(emb, dtype="float32")

    if isinstance(emb, str):
        try:
            return np.asarray(json.loads(emb), dtype="float32")
        except Exception:
            import ast
            return np.asarray(ast.literal_eval(emb), dtype="float32")

    return None


# -----------------------------
# Main search logic
# -----------------------------
def search_one_product(
    query_text: str,
    search_lang: str,
    supermarkets: List[str],
    sort_by: str = "unit_price",
    rpc_limit: int = 100,
    top_k: int = 50,
) -> Dict[str, Any]:

    supabase = get_supabase()

    # 1. Coarse search via Supabase RPC
    rpc_res = supabase.rpc(
        "search_products_ts",
        {
            "query_text": query_text,
            "search_lang": search_lang,
            "supermarkets": supermarkets,
            "sort_by": sort_by,
            "max_results": rpc_limit,
        },
    ).execute()

    rows = rpc_res.data or []
    if not rows:
        return {"query": query_text, "results": []}

    # 2. Get embedding from HF Space
    q_vec = get_embedding_from_hf(query_text)

    scored = []
    for r in rows:
        emb = r.get("embedding_du")
        v = parse_embedding(emb)
        if v is None:
            continue

        sim = float(np.dot(q_vec, v))

        new_r = dict(r)
        new_r["similarity"] = sim
        scored.append(new_r)

    # Sort by similarity
    if scored:
        scored.sort(key=lambda x: x["similarity"], reverse=True)
        candidates = scored[:top_k]
    else:
        candidates = rows

    # Sort by user preference (price)
    sort_by_lower = sort_by.lower()

    def price_key(row):
        if sort_by_lower == "current_price":
            price = row.get("current_price")
        else:
            price = row.get("unit_price")

        if price is None:
            return (1, float("inf"))
        return (0, float(price))

    candidates.sort(key=price_key)

    return {"query": query_text, "results": candidates}
