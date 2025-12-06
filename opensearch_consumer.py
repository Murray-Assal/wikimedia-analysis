'''
Opensearch Consumer for Wikimedia Recent Changes
This script consumes JSON events from a Kafka topic and indexes them into an OpenSearch index.
'''
import json
import logging
from opensearchpy import OpenSearch
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenSearch client configuration
opensearch_client = OpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    http_auth=("admin", "admin"),
    use_ssl=False,
    verify_certs=False,
    ssl_show_warn=False,
)

# Kafka consumer configuration
consumer = KafkaConsumer(
    "wikimedia.recentchange",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="opensearch-consumer-group"
)

def index_document(doc):
    """Index a document to OpenSearch"""
    try:
        response = opensearch_client.index(
            index="wikimedia",
            body=doc,
        )
        logger.info("Document indexed: %s", response['_id'])
    except Exception as e:
        logger.error("Error indexing document: %s", e)

if __name__ == "__main__":
    logger.info("Starting Kafka consumer for wikimedia.recentchange topic...")
    try:
        for message in consumer:
            index_document(message.value)
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
