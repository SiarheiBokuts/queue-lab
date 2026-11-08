import { describe, expect, it } from 'vitest'
import { containerState } from './containerState'

describe('containerState', () => {
  it('a running container is running', () => {
    expect(containerState({ state: 'running' })).toMatchObject({ label: 'Running', tone: 'ok' })
  })

  it('a sender that exited normally is done, not "exited"', () => {
    expect(containerState({ state: 'exited', exit_code: 0, role: 'producer' })).toMatchObject({ label: 'Done', tone: 'ok' })
  })

  it('a container stopped by the lab is stopped, not a failure', () => {
    for (const code of [137, 143]) expect(containerState({ state: 'exited', exit_code: code })).toMatchObject({ label: 'Stopped', tone: 'info' })
  })

  it('a container that ended with an error crashed and says which code', () => {
    const s = containerState({ state: 'exited', exit_code: 1 })
    expect(s).toMatchObject({ label: 'Crashed', tone: 'bad' })
    expect(s.hint).toContain('exit code 1')
  })

  it('running out of memory wins over everything else', () => {
    expect(containerState({ state: 'exited', exit_code: 137, oom: true })).toMatchObject({ label: 'Out of memory', tone: 'bad' })
  })

  it('unknown states are shown as they are', () => {
    expect(containerState({ state: 'weird' }).label).toBe('weird')
  })
})
