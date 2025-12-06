'''
Opensearch Consumer for Wikimedia Recent Changes
This script consumes JSON events from a Kafka topic and indexes them into an OpenSearch index.
'''
import json
import logging
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
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

def exract_id(change_event):
    """Extract unique ID from the change event"""
    return change_event.get("meta", {}).get("id")

def bulk_request(actions):
    """Perform bulk indexing to OpenSearch"""
    try:
        response = bulk(opensearch_client, actions)
        logger.info("Bulk indexing completed: %s", response)
    except Exception as e:
        logger.error("Error during bulk indexing: %s", e)

if __name__ == "__main__":
    logger.info("Starting Kafka consumer for wikimedia.recentchange topic...")
    try:
        for message in consumer:
            bulk_request([{
                "_index": "wikimedia",
                "_id": exract_id(message.value),
                "_source": message.value
            }])
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
