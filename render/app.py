from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

from search_logic import search_one_product
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # CORS
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchRequest(BaseModel):
    queries: List[str]
    search_lang: str = "du"
    supermarkets: List[str] = ["ah", "dirk", "hoogvliet"]
    sort_by: str = "unit_price"


@app.post("/search")
def search(req: SearchRequest):
    results = []
    for q in req.queries:
        res = search_one_product(
            query_text=q,
            search_lang=req.search_lang,
            supermarkets=req.supermarkets,
            sort_by=req.sort_by,
        )
        results.append(res)
    return {"results": results}


@app.get("/")
def health():
    return {"status": "ok", "message": "Render backend running"}
