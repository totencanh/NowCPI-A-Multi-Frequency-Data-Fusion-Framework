"""Evaluate isolated benchmark from per-microbatch post-write audit JSON."""
import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
import numpy as np


def parse(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00'))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--audit', default='workspace/benchmark_audit')
    p.add_argument('--output', default='forecasting/outputs')
    a = p.parse_args()
    audit_dir = Path(a.audit)
    files = sorted(audit_dir.glob('batch_*.json'))
    if not files:
        raise SystemExit(f'No audit files in {audit_dir}. Start benchmark and publish a batch first.')
    rows = []
    for file in files:
        entry = json.loads(file.read_text(encoding='utf-8'))
        bronze, gold = parse(entry['bronze_ready_at']), parse(entry['gold_ready_at'])
        for event in entry['events']:
            t0 = parse(event['emitted_at'])
            rows.append({'event_id': event['event_id'], 'batch_id': event['batch_id'],
                         'emitted_at':event['emitted_at'], 'bronze_ready_at':entry['bronze_ready_at'],
                         'gold_ready_at':entry['gold_ready_at'],
                         'freshness_sec':round((bronze-t0).total_seconds(),3),
                         'end_to_end_sec':round((gold-t0).total_seconds(),3)})
    # Deduplicate if a batch was replayed and audited twice.
    rows = list({r['event_id']:r for r in rows}.values())
    rows.sort(key=lambda r:r['emitted_at'])
    out = Path(a.output)
    out.mkdir(parents=True, exist_ok=True)
    with (out/'benchmark_events.csv').open('w', newline='',encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    fresh = np.array([r['freshness_sec'] for r in rows])
    e2e = np.array([r['end_to_end_sec'] for r in rows])
    metric = {'scope':'isolated_simplified_pipeline_post_write_proxy',
              'events':len(rows), 'source_batches':len({r['batch_id'] for r in rows}),
              'freshness_sla_60m_pct':round(100*np.mean((fresh >= 0)&(fresh <= 3600)),2),
              'freshness_p95_sec':round(float(np.percentile(fresh,95)),3),
              'end_to_end_p50_sec':round(float(np.percentile(e2e,50)),3),
              'end_to_end_p95_sec':round(float(np.percentile(e2e,95)),3),
              'end_to_end_p99_sec':round(float(np.percentile(e2e,99)),3),
              'end_to_end_max_sec':round(float(e2e.max()),3)}
    with (out/'benchmark_metrics.csv').open('w', newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=list(metric));w.writeheader();w.writerow(metric)
    print(json.dumps(metric,indent=2))
    print('Note: 10 events committed in one micro-batch are NOT 10 independent end-to-end batch latency observations.')

if __name__=='__main__':
    main()
