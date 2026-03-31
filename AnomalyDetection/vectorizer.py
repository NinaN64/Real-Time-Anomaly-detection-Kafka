import logging

import numpy as np
from sentence_transformers import SentenceTransformer

import config

log = logging.getLogger("vectorizer")


class Vectorizer:

    def __init__(self, model_name: str = config.SBERT_MODEL):
        log.info("Loading SBERT model: %s", model_name)
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        log.info("Model loaded. Embedding dimension: %d", self.embedding_dim)

    def embed_batch(self, batch: dict) -> np.ndarray:
        texts = [
            doc["cleanedText"]
            for doc in batch.get("documents", [])
            if doc.get("cleanedText", "").strip()
        ]

        if not texts:
            log.warning("Batch contained no embeddable text.")
            return np.empty((0, self.embedding_dim))

        embeddings = self.model.encode(
            texts,
            batch_size=64,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

        log.debug("Embedded %d documents -> shape %s", len(texts), embeddings.shape)
        return embeddings