from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import numpy as np
from sentence_transformers import SentenceTransformer

app = FastAPI()

print("[HF SPACE] loading MiniLM model ...")
model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
print("[HF SPACE] model loaded.")


# ------------------------------
# Request / Response schemas
# ------------------------------

class EmbedRequest(BaseModel):
    texts: List[str]


class EmbedResponse(BaseModel):
    embeddings: List[List[float]]


# ------------------------------
# Routes
# ------------------------------

@app.get("/")
def health_check():
    return {"status": "ok", "detail": "embedding API is running"}


@app.post("/embed", response_model=EmbedResponse)
def embed(req: EmbedRequest):
    if not req.texts:
        return {"embeddings": []}

    # Compute embeddings
    emb = model.encode(
        req.texts,
        batch_size=16,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    # Convert to python lists
    emb_list = emb.astype("float32").tolist()

    return {"embeddings": emb_list}
