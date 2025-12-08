# backend/embed_existing_products.py

from __future__ import annotations

from typing import List, Dict, Any

from sentence_transformers import SentenceTransformer

from supabase_utils import get_supabase, upsert_rows

from dotenv import load_dotenv
load_dotenv()

# --------------------------------------------------------------------
# 1. Load Sentence Transformer model
# --------------------------------------------------------------------
print("[EMB] loading model...")
EMBED_MODEL = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
print("[EMB] model loaded.")


def encode_texts(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []

    embs = EMBED_MODEL.encode(
        texts,
        normalize_embeddings=True,
        batch_size=64,
        show_progress_bar=True,
    )
 
    return [[float(x) for x in vec] for vec in embs]


# --------------------------------------------------------------------
#  Embed the brand + product_name_du 
# --------------------------------------------------------------------
def process_table(table_name: str, batch_size: int = 200):
    total_updated = 0
    print(f"\n[EMB] start table={table_name}")

    while True:
        supabase = get_supabase()

        res = (
            supabase.table(table_name)
            .select("sku, brand, product_name_du, embedding_du")
            .is_("embedding_du", "null")
            .not_.is_("product_name_du", "null")
            .limit(batch_size)
            .execute()
        )

        rows: List[Dict[str, Any]] = res.data or []
        if not rows:
            break

        print(f"[EMB] {table_name}: fetched {len(rows)} rows")

        texts: List[str] = []
        skus: List[str] = []

        for r in rows:
            brand = (r.get("brand") or "").strip()
            name = (r.get("product_name_du") or "").strip()

            if not name:
                continue

            text = (brand + " " + name).strip()
            if not text:
                continue

            skus.append(str(r["sku"]))
            texts.append(text)

        if not skus:
            print(f"[EMB] {table_name}: all {len(rows)} rows in this batch have empty names, done.")
            break

        embs = encode_texts(texts)

        updates = [
            {"sku": sku, "embedding_du": emb}
            for sku, emb in zip(skus, embs)
        ]

        upsert_rows(table_name, updates, conflict_col="sku")

        updated_count = len(updates)
        total_updated += updated_count
        print(f"[EMB] {table_name}: upserted {updated_count} rows, total={total_updated}")

    print(f"[EMB] DONE table={table_name}, total_updated={total_updated}")




# --------------------------------------------------------------------
# 3. main： ah / dirk / hoogvliet
# --------------------------------------------------------------------
if __name__ == "__main__":

    process_table("ah")
    process_table("dirk")
    process_table("hoogvliet")