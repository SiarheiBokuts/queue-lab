import { siGo, siNodedotjs, siOpenjdk, siPython, siRust } from 'simple-icons'

/** The worker languages. Every one has a producer and a consumer behind the same contract (see /workers). */
export interface Lang {
  id: string
  name: string
  /** Brand-like colour used for chips and chart lines, so a language looks the same everywhere. */
  color: string
  /** The client library, shown as a hint: only the language and its client differ between runs. */
  client: string
  /** SVG path (24x24) of the language's logo, from simple-icons. */
  icon: string
}

export const LANGS: Lang[] = [
  { id: 'python', name: 'Python', color: '#4b8bbe', client: 'confluent-kafka (librdkafka)', icon: siPython.path },
  { id: 'node', name: 'Node.js', color: '#68a063', client: '@confluentinc/kafka-javascript (librdkafka)', icon: siNodedotjs.path },
  { id: 'java', name: 'Java', color: '#f89820', client: 'Apache kafka-clients', icon: siOpenjdk.path },
  { id: 'go', name: 'Go', color: '#00add8', client: 'confluent-kafka-go (librdkafka)', icon: siGo.path },
  { id: 'rust', name: 'Rust', color: '#dea584', client: 'rdkafka (librdkafka)', icon: siRust.path },
]

export const langName = (id: string): string => LANGS.find((l) => l.id === id)?.name ?? id
export const langColor = (id: string): string => LANGS.find((l) => l.id === id)?.color ?? '#a1a1aa'

/**
 * The worker group in words. "At once" only means something while a message waits (the delay scenario): a message that
 * computes keeps its core busy, so it cannot be overlapped with others, and echo has nothing to wait for.
 */
export function workersText(w: { count: number; lang: string; concurrency?: number; ordering?: string }, scenario?: string): string {
  const base = `${w.count} × ${langName(w.lang)} workers`
  if (scenario === 'cpu') return `${base} (computing: each keeps about one core busy)`
  if (scenario === 'echo') return base
  if (!w.concurrency) return base
  const per = w.concurrency <= 1
    ? 'one message at a time each'
    : `each handles up to ${w.concurrency} at once${w.ordering === 'none' ? ', any order' : ', in order per user'}`
  return `${base} (${per})`
}