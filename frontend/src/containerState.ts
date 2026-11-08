/** Docker's own words ("exited", "created") say little. This turns them into states a person understands. */

export type Tone = 'ok' | 'warn' | 'bad' | 'info'
export interface ContainerState { label: string; tone: Tone; hint: string }

interface Box { state: string; oom?: boolean; exit_code?: number | null; role?: string }

/** Exit codes a stopped lab container gets when the lab itself stops it (SIGTERM and SIGKILL). */
const STOPPED_BY_LAB = [137, 143]

export function containerState(c: Box): ContainerState {
  if (c.oom) return { label: 'Out of memory', tone: 'bad', hint: 'Docker stopped it because it used more memory than its limit. Give it more memory in the form and run again.' }
  switch (c.state) {
    case 'running':
      return { label: 'Running', tone: 'ok', hint: 'It is working now.' }
    case 'created':
    case 'restarting':
      return { label: 'Starting', tone: 'info', hint: 'It is being started.' }
    case 'paused':
      return { label: 'Paused', tone: 'warn', hint: 'It is paused and does nothing until it is resumed.' }
    case 'exited': {
      const code = c.exit_code ?? 0
      if (code === 0) {
        return c.role === 'producer'
          ? { label: 'Done', tone: 'ok', hint: 'The sender sent all its messages and finished normally.' }
          : { label: 'Stopped', tone: 'info', hint: 'It stopped normally.' }
      }
      if (STOPPED_BY_LAB.includes(code)) return { label: 'Stopped', tone: 'info', hint: 'The lab stopped it, at the end of a run or on Reset.' }
      return { label: 'Crashed', tone: 'bad', hint: `It stopped with an error (exit code ${code}). Open the container to read its log.` }
    }
    case 'dead':
    case 'removing':
      return { label: 'Removed', tone: 'info', hint: 'It is being removed.' }
    default:
      return { label: c.state || 'Unknown', tone: 'info', hint: 'Docker reports this state.' }
  }
}
