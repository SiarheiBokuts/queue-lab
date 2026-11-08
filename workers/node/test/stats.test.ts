import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ConsumerStats } from '../src/stats.js';

const EDGES = [1, 10, 100];
const header = (seq: number, sentMs: number) => ({ producer: 0, partition: 2, seq, sentMs });

describe('ConsumerStats', () => {
  it('counts messages, latency buckets and the slowest message', () => {
    const s = new ConsumerStats(EDGES);
    s.record(header(0, 995), 2, 1000); // 5 ms
    s.record(header(1, 700), 2, 1000); // 300 ms: above the last edge
    assert.equal(s.received, 2);
    assert.equal(s.maxLatencyMs, 300);
    const r = s.report({ runId: 'r', worker: 'w' }, [2], false);
    assert.deepEqual(r.hist, [0, 1, 0, 1]);
  });

  it('reports the numbers that arrived as ranges and a repeat as a duplicate', () => {
    const s = new ConsumerStats(EDGES);
    s.record(header(0, 1000), 2, 1000);
    s.record(header(2, 1000), 2, 1000); // 1 is missing
    s.record(header(2, 1000), 2, 1000); // 2 again
    const r = s.report({ runId: 'r', worker: 'w' }, [2], false);
    assert.deepEqual(r.ranges, { '0:2': [[0, 0], [2, 2]] }); // 1 is in no range: the lab counts it as lost
    assert.equal(r.duplicates, 1);
  });

  it('sends the histogram as the change since the last report', () => {
    const s = new ConsumerStats(EDGES);
    s.record(header(0, 1000), 2, 1000);
    s.report({ runId: 'r', worker: 'w' }, [2], false);
    const second = s.report({ runId: 'r', worker: 'w' }, [2], true);
    assert.deepEqual(second.hist, [0, 0, 0, 0]);
    assert.equal(second.received, 1); // the other counters are totals
    assert.equal(second.final, true);
  });
});