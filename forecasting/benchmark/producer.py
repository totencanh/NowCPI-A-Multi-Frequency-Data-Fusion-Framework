"""Publish timestamped events to an isolated Kafka benchmark topic."""
import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--broker', default='localhost:9092')
    p.add_argument('--topic', default='nowcpi.benchmark.events')
    p.add_argument('--count', type=int, default=10)
    p.add_argument('--batches', type=int, default=1)
    p.add_argument('--interval', type=float, default=15)
    a = p.parse_args()
    if a.count < 1 or a.batches < 1:
        p.error('count and batches must be positive')
    producer = Producer({'bootstrap.servers': a.broker, 'client.id': 'nowcpi-benchmark'})
    for b in range(a.batches):
        batch_id = f"bench-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}"
        events = []
        for i in range(a.count):
            evt = {'batch_id': batch_id, 'event_id': f'{batch_id}-{i}',
                   'emitted_at': utc_now(), 'series_id': 'benchmark',
                   'value': float(i + 1), 'sequence': i}
            producer.produce(a.topic, key=evt['event_id'], value=json.dumps(evt))
            events.append(evt)
        undelivered = producer.flush(30)
        if undelivered:
            raise RuntimeError(f'{undelivered} messages not delivered')
        print(json.dumps({'batch_id':batch_id, 'events':len(events), 'first_emitted_at':events[0]['emitted_at'], 'last_emitted_at':events[-1]['emitted_at']}), flush=True)
        if b + 1 < a.batches:
            time.sleep(a.interval)

if __name__ == '__main__':
    main()
