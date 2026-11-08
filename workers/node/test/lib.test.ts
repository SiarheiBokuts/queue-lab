import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { SeqTracker, bucket, buildHeader, pacingChunk, pacingDelay, parseEdges, parseHeader, laneFor, userKey, makeHeaderParser } from '../src/lib.js';

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

describe('SeqTracker', () => {
  it('accepts an in-order sequence', () => {
    const t = new SeqTracker();
    for (let s = 0; s < 5; s++) assert.equal(t.observe('0:0', s), 'ok');
    assert.equal(t.gaps + t.duplicates, 0);
  });

  it('counts a gap', () => {
    const t = new SeqTracker();
    t.observe('0:0', 0);
    assert.equal(t.observe('0:0', 4), 'gap');
    assert.equal(t.gaps, 3);
    assert.equal(t.observe('0:0', 5), 'ok');
  });

  it('counts duplicates', () => {
    const t = new SeqTracker();
    t.observe('0:1', 0);
    t.observe('0:1', 1);
    assert.equal(t.observe('0:1', 1), 'duplicate');
    assert.equal(t.observe('0:1', 0), 'duplicate');
    assert.equal(t.duplicates, 2);
  });

  it('tracks partitions independently', () => {
    const t = new SeqTracker();
    t.observe('0:0', 0);
    t.observe('0:1', 0);
    assert.equal(t.observe('0:0', 1), 'ok');
    assert.equal(t.observe('0:1', 1), 'ok');
  });

  it('uses the first message only as a baseline', () => {
    const t = new SeqTracker();
    assert.equal(t.observe('0:2', 500), 'ok');
    assert.equal(t.gaps, 0);
  });
});

describe('streams of different producers', () => {
  it('do not clash: both number their messages from 0 on the same partition', () => {
    const t = new SeqTracker();
    for (let seq = 0; seq < 3; seq++) {
      assert.equal(t.observe('0:5', seq), 'ok');
      assert.equal(t.observe('1:5', seq), 'ok');
    }
    assert.equal(t.gaps + t.duplicates, 0);
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

describe('users, keys and lanes', () => {
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

  it('the same key always gets the same lane, within range', () => {
    assert.equal(laneFor('user-7', 16), laneFor('user-7', 16));
    for (let n = 0; n < 200; n++) {
      const lane = laneFor(`user-${n}`, 8);
      assert.ok(lane >= 0 && lane < 8);
    }
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
