from __future__ import annotations

from typing import Any
from typing import cast

import chromadb

from chromadb.utils import embedding_functions
from dotenv import load_dotenv

from swiss_exp_tracker.pipeline_agentic.data_models.merchant import CategoryMain
from swiss_exp_tracker.pipeline_agentic.data_models.merchant import MerchantMetaData

load_dotenv(override=True)

PTH_CHROMA = "./merchant_vector_store"


class MerchantStore:
    def __init__(self, path: str | None = None) -> None:
        """Initialise the Chroma persistent store; path defaults to PTH_CHROMA."""
        self.client = chromadb.PersistentClient(
            path=path if path is not None else PTH_CHROMA
        )
        self.ef = embedding_functions.DefaultEmbeddingFunction()
        self.collection = self.client.get_or_create_collection(
            name="merchants",
            embedding_function=self.ef,  # type: ignore[arg-type]
            metadata={"hnsw:space": "cosine"},  # use cosine similarity
        )

    def _best_match(
        self,
        merchant_raw: str,
        include_metadata: bool = True,
    ) -> tuple[str, float, dict[str, Any] | None] | None:
        if self.collection.count() == 0:
            return None

        if include_metadata:
            results = self.collection.query(
                query_texts=[merchant_raw],
                n_results=1,
                include=["distances", "metadatas"],
            )
        else:
            results = self.collection.query(
                query_texts=[merchant_raw],
                n_results=1,
                include=["distances"],
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
            distance = float(distances[0][0])
            similarity = 1.0 - distance
            metadata = (
                cast("dict[str, Any]", metadatas[0][0])
                if include_metadata and metadatas
                else None
            )
            return match_id, similarity, metadata
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def _to_merchant_metadata(self, metadata: dict[str, Any]) -> MerchantMetaData:
        return MerchantMetaData(
            name=str(metadata["name"]),
            category_main=CategoryMain(metadata["category_main"]),
            category_second=str(metadata["category_second"]),
            city=str(metadata["city"]),
        )

    def search(
        self, merchant_raw: str, threshold: float = 0.88
    ) -> tuple[MerchantMetaData, float] | None:
        best_match = self._best_match(merchant_raw, include_metadata=True)
        if best_match is None:
            return None

        _, similarity, metadata = best_match
        if similarity < threshold or metadata is None:
            return None

        return self._to_merchant_metadata(metadata), similarity

    def save(self, merchant_raw: str, summary: str, metadata: MerchantMetaData) -> None:
        if not merchant_raw:
            return
        meta_dict = metadata.model_dump()

        # Flatten: chroma only accepts str/int/float/bool values in metadata
        flat_meta = {
            k: v.value if hasattr(v, "value") else (str(v) if v is not None else "")
            for k, v in meta_dict.items()
        }

        best_match = self._best_match(merchant_raw, include_metadata=False)
        existing_id = (
            best_match[0] if best_match and best_match[1] >= 0.93 else merchant_raw
        )

        self.collection.upsert(
            ids=[existing_id],
            documents=[merchant_raw],
            metadatas=[{"merchant_raw": merchant_raw, "summary": summary, **flat_meta}],
        )
