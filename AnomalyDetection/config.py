# Kafka
BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC_TUMBLING = "preprocessed-batches-tumbling"
INPUT_TOPIC_SLIDING  = "preprocessed-batches-sliding"
ALERT_TOPIC          = "drift-alerts"
CONSUMER_GROUP       = "anomaly-detection-group"

WINDOW_MODE = "tumbling"  # "tumbling" or "sliding"

# Vectorizer
SBERT_MODEL = "all-MiniLM-L6-v2"

# Embedding store
EMBEDDING_STORE_SIZE = 500
MIN_EMBEDDINGS_BEFORE_DETECTION = 100

# Active detectors
ACTIVE_DETECTORS = ["mmd", "isolation_forest"]

# MMD
MMD_THRESHOLD   = 0.05
MMD_SAMPLE_SIZE = 200

# Isolation Forest
IF_CONTAMINATION = 0.05
IF_N_ESTIMATORS  = 100
IF_THRESHOLD     = -0.6