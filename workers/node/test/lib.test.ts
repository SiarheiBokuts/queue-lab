import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { MAX_RANGES, SeqRanges, bucket, buildHeader, pacingChunk, pacingDelay, parseEdges, parseHeader, groupByUser, userKey, makeHeaderParser } from '../src/lib.js';

const EDGES = [0.5, 1, 2, 5, 10];

describe('parseEdges', () => {
  it('parses a comma separated list', () => {
    assert.deepEqual(parseEdges('0.5,1,2,5'), [0.5, 1, 2, 5]);
    assert.deepEqual(parseEdges(' 1 , 2 ,'), [1, 2]);
  });
});

describe('bucket', () => {
  const cases: Array<[number, number]> = [
    [0, 0], [0.5, 0], [0.51, 1], [1, 1], [4.9, 3], [5, 3], [10, 4], [10.1, 5], [99999, 5],
  ];
  for (const [latency, expected] of cases) {
    it(`puts ${latency} ms into bucket ${expected}`, () => {
      assert.equal(bucket(latency, EDGES), expected);
    });
  }
});

describe('header', () => {
  it('round trips', () => {
    const message = buildHeader('abc12345', 2, 3, 42, 1794147580123.456) + 'x'.repeat(100);
    assert.deepEqual(parseHeader(message.slice(0, 96), 'abc12345'), { producer: 2, partition: 3, seq: 42, sentMs: 1794147580123.456 });
  });

  it('ignores a message of another run', () => {
    assert.equal(parseHeader(buildHeader('other', 0, 0, 0, 1), 'abc12345'), null);
  });

  it('rejects malformed headers', () => {
    for (const garbage of ['', 'no separators', 'a|b|c', 'run|0|x|y|z|pad', 'run|0|1|2|notanumber|pad']) {
      assert.equal(parseHeader(garbage, 'run'), null, garbage);
    }
  });
});

describe('SeqRanges', () => {
  it('numbers one after another make one range', () => {
    const seen = new SeqRanges();
    for (let s = 0; s < 5; s++) assert.equal(seen.observe(0, 0, s), true);
    assert.deepEqual(seen.report(), { '0:0': [[0, 4]] });
    assert.equal(seen.repeats + seen.overflow, 0);
  });

  it('a skipped number leaves a gap between two ranges', () => {
    const seen = new SeqRanges();
    for (const s of [0, 1, 2, 5, 6]) seen.observe(0, 0, s);
    assert.deepEqual(seen.report(), { '0:0': [[0, 2], [5, 6]] }); // 3 and 4 are lost
  });

  it('a number seen twice is a repeat', () => {
    const seen = new SeqRanges();
    for (let s = 0; s < 3; s++) seen.observe(1, 0, s);
    assert.equal(seen.observe(1, 0, 1), false);
    assert.equal(seen.observe(1, 0, 0), false);
    assert.equal(seen.repeats, 2);
  });

  it('a replayed batch fills a gap and the ranges are merged', () => {
    const seen = new SeqRanges();
    for (const s of [0, 1, 4, 5]) seen.observe(0, 0, s);
    for (const s of [2, 3]) assert.equal(seen.observe(0, 0, s), true);
    assert.deepEqual(seen.report(), { '0:0': [[0, 5]] });
  });

  it('streams of different senders and partitions do not clash', () => {
    const seen = new SeqRanges();
    for (let seq = 0; seq < 3; seq++) {
      assert.ok(seen.observe(0, 5, seq) && seen.observe(1, 5, seq) && seen.observe(0, 6, seq));
    }
    assert.equal(seen.repeats, 0);
    assert.deepEqual(Object.keys(seen.report()).sort(), ['0:5', '0:6', '1:5']);
  });

  it('a stream with too many gaps stops listing but keeps counting', () => {
    const seen = new SeqRanges();
    for (let s = 0; s < MAX_RANGES * 2 + 10; s += 2) seen.observe(0, 0, s);
    assert.equal(seen.report()['0:0']?.length, MAX_RANGES);
    assert.ok(seen.overflow > 0);
  });
});
describe('pacing', () => {
  it('does not wait without a rate', () => {
    assert.equal(pacingDelay(1_000_000, 0, 0.001), 0);
  });

  it('waits for the difference when ahead of the rate', () => {
    assert.equal(pacingDelay(1000, 1000, 0.25), 0.75);
  });

  it('never waits when behind schedule', () => {
    assert.equal(pacingDelay(1000, 1000, 2), 0);
  });

  it('checks the clock about every ten milliseconds of messages', () => {
    assert.equal(pacingChunk(0, 1000), 1000);
    assert.equal(pacingChunk(10_000, 1000), 100);
    assert.equal(pacingChunk(5_000_000, 1000), 1000);
    assert.equal(pacingChunk(50, 1000), 1);
  });
});

describe('users, keys and groups', () => {
  it('a user always lands in the same partition and every user appears', () => {
    const seen = new Map<string, number>();
    for (let i = 0; i < 500; i++) {
      const part = i % 6;
      const key = userKey(part, i, 6, 60);
      assert.equal(seen.get(key) ?? part, part);
      seen.set(key, part);
    }
    assert.equal(seen.size, 60);
  });

  it('messages are grouped by user and keep their order', () => {
    const key = (name: string) => Buffer.from(name);
    const batch = [{ key: key('user-1'), n: 1 }, { key: key('user-2'), n: 2 }, { key: key('user-1'), n: 3 }, { key: key('user-1'), n: 4 }];
    assert.deepEqual(groupByUser(batch).map((g) => g.map((m) => m.n)), [[1, 3, 4], [2]]);
    assert.deepEqual(groupByUser([]), []);
  });
});
describe('makeHeaderParser (reads the bytes directly)', () => {
  const parse = makeHeaderParser('run1');

  it('gives the same result as the readable parseHeader', () => {
    for (const [producer, partition, seq, sent] of [[0, 0, 0, 1794170000123.456], [3, 41, 987654321, 5.5], [7, 63, 1, 1794170000000.0]] as const) {
      const text = buildHeader('run1', producer, partition, seq, sent);
      const buf = Buffer.from(text + 'x'.repeat(100), 'latin1');
      const fast = parse(buf);
      const slow = parseHeader(text, 'run1');
      assert.ok(fast !== null && slow !== null);
      assert.equal(fast.producer, slow.producer);
      assert.equal(fast.partition, slow.partition);
      assert.equal(fast.seq, slow.seq);
      assert.ok(Math.abs(fast.sentMs - slow.sentMs) < 0.001);
    }
  });

  it('ignores other runs and broken headers', () => {
    assert.equal(parse(Buffer.from('other|0|0|0|1.0|xx', 'latin1')), null);
    assert.equal(parse(Buffer.from('run1|a|0|0|1.0|xx', 'latin1')), null);
    assert.equal(parse(Buffer.from('run1|0|0|0|', 'latin1')), null);
    assert.equal(parse(Buffer.from('run1', 'latin1')), null);
  });
});
