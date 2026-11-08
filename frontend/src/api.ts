async function call(method: string, url: string, body?: unknown) {
  const res = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    const detail = data.detail
    const msg = typeof detail === 'string' ? detail : Array.isArray(detail) ? detail.map((d: any) => String(d.msg).replace(/^Value error, /, '')).join('; ') : `Request failed (${res.status})`
    throw new Error(msg)
  }
  return data
}

export const api = {
  state: () => call('GET', '/api/state'),
  plan: (body: unknown) => call('POST', '/api/plan', body),
  start: (body: unknown) => call('POST', '/api/setups', body),
  cancel: () => call('POST', '/api/runs/current/cancel'),
  runs: () => call('GET', '/api/runs'),
  run: (id: string) => call('GET', `/api/runs/${id}`),
  deleteRun: (id: string) => call('DELETE', `/api/runs/${id}`),
  deleteRuns: () => call('DELETE', '/api/runs'),
  settings: (body: unknown) => call('PUT', '/api/settings', body),
  deleteQueue: (name: string) => call('DELETE', `/api/queues/${name}`),
  logs: (name: string, tail = 200) => call('GET', `/api/containers/${name}/logs?tail=${tail}`),
  reset: () => call('POST', '/api/reset'),
  scenarios: () => call('GET', '/api/scenarios'),
  startComparison: (body: unknown) => call('POST', '/api/comparisons', body),
  compare: (ids: string) => call('GET', `/api/compare?ids=${ids}`),
  code: () => call('GET', '/api/code'),
  currentSamples: () => call('GET', '/api/runs/current/samples'),
  currentFull: () => call('GET', '/api/runs/current/full'),
}
