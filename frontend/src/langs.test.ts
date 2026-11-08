import { describe, expect, it } from 'vitest'
import { LANGS, langColor, langName, workersText } from './langs'

describe('languages', () => {
  it('offers every worker language with a name, a colour and its Kafka client', () => {
    expect(LANGS.map((l) => l.id)).toEqual(['python', 'node', 'java', 'go', 'rust'])
    for (const l of LANGS) expect(l.name && l.color && l.client).toBeTruthy()
  })

  it('names a language by id and falls back to the id', () => {
    expect(langName('node')).toBe('Node.js')
    expect(langName('cobol')).toBe('cobol')
    expect(langColor('cobol')).toMatch(/^#/)
  })


  it('says how many messages each worker handles at once', () => {
    expect(workersText({ count: 5, lang: 'rust', concurrency: 100 })).toBe("5 × Rust workers (each handles up to 100 at once, a user's messages in order)")
    expect(workersText({ count: 3, lang: 'go', concurrency: 1 })).toBe('3 × Go workers (one message at a time each)')
    expect(workersText({ count: 2, lang: 'node' })).toBe('2 × Node.js workers')
    expect(workersText({ count: 5, lang: 'python', concurrency: 100 }, 'json')).toBe('5 × Python workers (parsing JSON: each keeps about one core busy)')
    expect(workersText({ count: 5, lang: 'python', concurrency: 100 }, 'echo')).toBe('5 × Python workers')
    expect(workersText({ count: 5, lang: 'python', concurrency: 100 }, 'delay')).toContain('up to 100 at once')
  })
})