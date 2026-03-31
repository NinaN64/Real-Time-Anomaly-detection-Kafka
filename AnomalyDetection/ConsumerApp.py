import logging

import config
from consumer import run as run_consumer
from vectorizer import Vectorizer
from embedding_store import EmbeddingStore
from alert_publisher import AlertPublisher
from detectors import load_detectors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("ConsumerApp")


def main():
    log.info("Starting AnomalyDetection pipeline")
    log.info("Window mode:      %s", config.WINDOW_MODE)
    log.info("SBERT model:      %s", config.SBERT_MODEL)
    log.info("Store size:       %d", config.EMBEDDING_STORE_SIZE)
    log.info("Active detectors: %s", config.ACTIVE_DETECTORS)

    vectorizer = Vectorizer()
    store      = EmbeddingStore()
    publisher  = AlertPublisher()
    detectors  = load_detectors(config.ACTIVE_DETECTORS)

    def handle_batch(batch: dict) -> None:
        embeddings = vectorizer.embed_batch(batch)
        if embeddings.shape[0] == 0:
            return

        reference = store.get_reference()
        store.add(embeddings)

        if not store.is_ready():
            log.info("Warming up... (%d/%d embeddings)",
                     store.total_seen, config.MIN_EMBEDDINGS_BEFORE_DETECTION)
            return

        if reference.shape[0] == 0:
            return

        for detector in detectors:
            alert = detector.update(embeddings, reference)
            if alert:
                log.warning(
                    "DRIFT ALERT | detector=%s | score=%.4f | window=[%d, %d]",
                    alert["detector"], alert["score"],
                    batch.get("windowStart", 0), batch.get("windowEnd", 0)
                )
                publisher.publish(alert, batch)

    try:
        run_consumer(handle_batch, window_mode=config.WINDOW_MODE)
    finally:
        publisher.flush()
        log.info("Pipeline shut down.")


if __name__ == "__main__":
    main()