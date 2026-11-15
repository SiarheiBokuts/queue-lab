import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { makeHeaderParser } from '../src/lib.js';
import { MessageFactory } from '../src/message.js';

const options = { run: 'run1', producerIndex: 1, partitions: 4, size: 128, users: 40, randomPayload: false };

describe('MessageFactory', () => {
  it('makes messages of the chosen size with a header the consumer can read', () => {
    const parse = makeHeaderParser('run1');
    const m = new MessageFactory(options).make(0);
    assert.equal(m.value.length, 128);
    const header = parse(m.value);
    assert.ok(header !== null);
    assert.equal(header.producer, 1);
    assert.equal(header.partition, m.partition);
    assert.equal(header.seq, 0);
  });

  it('goes round the partitions and counts the sequence per partition', () => {
    const parse = makeHeaderParser('run1');
    const batch = new MessageFactory(options).batch(0, 9);
    assert.deepEqual(
      batch.map((m) => m.partition),
      [1, 2, 3, 0, 1, 2, 3, 0, 1],
    );
    const seqOfLast = parse(batch[8]?.value as Buffer)?.seq;
    assert.equal(seqOfLast, 2); // the third message of partition 1
  });

  it('keeps a user in one partition', () => {
    const factory = new MessageFactory(options);
    const partitionOf = new Map<string, number>();
    for (const m of factory.batch(0, 400)) {
      assert.equal(partitionOf.get(m.key) ?? m.partition, m.partition);
      partitionOf.set(m.key, m.partition);
    }
    assert.equal(partitionOf.size, 40);
  });

  it('pads random messages to the same size', () => {
    const m = new MessageFactory({ ...options, randomPayload: true }).make(5);
    assert.equal(m.value.length, 128);
  });
});
