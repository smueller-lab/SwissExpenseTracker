from __future__ import annotations

from typing import Any
from typing import cast

import chromadb

from chromadb.utils import embedding_functions
from dotenv import load_dotenv

from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryData
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryDetail
from swiss_exp_tracker.pipeline_agentic.data_models.grocery import GroceryCategoryMain

load_dotenv(override=True)

PTH_CHROMA = "./grocery_vector_store"


class GroceryStore:
    def __init__(self) -> None:
        self.client = chromadb.PersistentClient(path=PTH_CHROMA)
        self.ef = embedding_functions.DefaultEmbeddingFunction()
        self.collection = self.client.get_or_create_collection(
            name="groceries",
            embedding_function=self.ef,  # type: ignore[arg-type]
            metadata={"hnsw:space": "cosine"},
        )

    def _best_match(
        self,
        article_normalized: str,
        include_metadata: bool = True,
    ) -> tuple[str, float, dict[str, Any] | None] | None:
        if self.collection.count() == 0:
            return None

        include: list[str] = ["distances"]
        if include_metadata:
            include.append("metadatas")

        results = self.collection.query(
            query_texts=[article_normalized],
            n_results=1,
            include=include,  # type: ignore[arg-type]
        )

        ids = results.get("ids")
        distances = results.get("distances")
        if not ids or not ids[0] or not distances or not distances[0]:
            return None

        metadatas = results.get("metadatas")
        if include_metadata and (not metadatas or not metadatas[0]):
            return None

        try:
            match_id = str(ids[0][0])
            similarity = 1.0 - float(distances[0][0])
            metadata = (
                cast("dict[str, Any]", metadatas[0][0])
                if include_metadata and metadatas
                else None
            )
            return match_id, similarity, metadata
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def _to_category_data(self, metadata: dict[str, Any]) -> GroceryCategoryData:
        return GroceryCategoryData(
            article=str(metadata["article_normalized"]),
            category_main=GroceryCategoryMain(metadata["category_main"]),
            category_detail=GroceryCategoryDetail(metadata["category_detail"]),
        )

    def search(
        self, article_normalized: str, threshold: float = 0.85
    ) -> tuple[GroceryCategoryData, float] | None:
        best = self._best_match(article_normalized, include_metadata=True)
        if best is None:
            return None
        _, similarity, metadata = best
        if similarity < threshold or metadata is None:
            return None
        return self._to_category_data(metadata), similarity

    def save(self, article_normalized: str, category: GroceryCategoryData) -> None:
        flat_meta = {
            "article_normalized": article_normalized,
            "category_main": category.category_main.value,
            "category_detail": category.category_detail.value,
        }

        best = self._best_match(article_normalized, include_metadata=False)
        existing_id = best[0] if best and best[1] >= 0.93 else article_normalized

        self.collection.upsert(
            ids=[existing_id],
            documents=[article_normalized],
            metadatas=[flat_meta],
        )
