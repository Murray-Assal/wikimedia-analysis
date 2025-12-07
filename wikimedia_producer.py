
#!/usr/bin/env python3
"""
wikimedia_producer.py

Simple Kafka producer that reads the Wikimedia "recentchange" SSE stream and
publishes each event as JSON to a Kafka topic.

Dependencies:
 - requests
 - kafka-python

Install: pip install requests kafka-python
"""
import argparse
import json
import logging
import signal
import sys
from typing import List
import requests
from kafka import KafkaProducer



STOP = False


def signal_handler(sig, frame):
    '''
    Docstring for signal_handler
    
    :param sig: Signal number
    :param frame: Current stack frame
    '''
    global STOP
    STOP = True
    logging.info("Shutdown requested, stopping...")


def sse_event_lines(resp) -> List[str]:
    """
    Generator that yields SSE logical lines (including blank lines).
    We rely on requests.iter_lines() to return '' for blank lines.
    """
    for line in resp.iter_lines(decode_unicode=True):
        # iter_lines may yield None or skip some blank lines depending on server/requests;
        # treat falsy values as blank separator.
        yield line if line is not None else ""


def run_producer(bootstrap_servers: str, topic: str, stream_url: str, group_id: str = None):
    '''
    Docstring for run_producer
    
    :param bootstrap_servers: Comma-separated list of Kafka bootstrap servers
    :param topic: Kafka topic to produce to
    :param stream_url: URL of the Wikimedia recentchange SSE stream
    :param group_id: Kafka consumer group ID (optional)
    '''
    logging.info("Connecting to Kafka at %s, producing to topic '%s'", bootstrap_servers, topic)

    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",")],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=32 * 1024,  # 32KB
        compression_type="snappy"
    )

    headers = {
    "Accept": "text/event-stream",
    "User-Agent": "MuradAsalKafkaProducer/1.0 (https://https://github.com/Murray-Assal/wikimedia-analysis)"
    }
    with requests.get(stream_url, headers=headers, stream=True, timeout=60) as resp:
        if resp.status_code != 200:
            logging.error("Failed to connect to stream: %s %s", resp.status_code, resp.text)
            return

        logging.info("Connected to Wikimedia stream, starting to consume events...")
        event_lines = []
        for raw_line in sse_event_lines(resp):
            if STOP:
                break

            # Some libraries return bytes or str; ensure str
            line = raw_line if isinstance(raw_line, str) else (raw_line.decode("utf-8") if raw_line else "")

            # According to SSE: empty line -> dispatch event
            if line == "":
                if not event_lines:
                    continue
                # join multiple data: lines (if present)
                data_parts = []
                for l in event_lines:
                    if l.startswith("data:"):
                        data_parts.append(l[len("data:"):].lstrip())
                payload = "\n".join(data_parts).strip()
                event_lines = []

                if not payload:
                    continue

                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    logging.debug("Non-JSON event received, sending raw payload")
                    obj = {"raw": payload}

                try:
                    producer.send(topic, value=obj)
                    # optional: flush periodically or rely on background thread
                except Exception as e:
                    logging.exception("Failed to send message to Kafka: %s", e)
                continue

            # accumulate lines for current event
            event_lines.append(line)

    logging.info("Flushing producer and closing...")
    try:
        producer.flush(timeout=10)
    except Exception:
        pass
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wikimedia recentchange -> Kafka producer")
    parser.add_argument(
        "--bootstrap-servers",
        "-b",
        default="localhost:29092",
        help="Comma-separated Kafka bootstrap servers (default: localhost:29092)",
    )
    parser.add_argument("--topic", "-t", default="wikimedia.recentchange", help="Kafka topic to produce to")
    parser.add_argument(
        "--stream-url",
        default="https://stream.wikimedia.org/v2/stream/recentchange",
        help="Wikimedia SSE stream URL",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        run_producer(args.bootstrap_servers, args.topic, args.stream_url)
    except KeyboardInterrupt:
        pass
    except Exception:
        logging.exception("Producer crashed")
        sys.exit(1)
