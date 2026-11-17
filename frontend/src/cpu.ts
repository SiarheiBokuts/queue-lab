/**
 * How many cores every sender and worker container gets. The same rule as `fair_cpu` in backend/app/budget.py (keep in
 * sync): the cores of Docker minus the ones kept for Kafka and the app, shared equally by all containers, at least a
 * quarter of a core and at most the maximum, rounded down to a twentieth of a core. The two numbers come from the
 * backend (`limits` in /api/state, defined in config.py), not from here.
 */
export interface CpuLimits {
  cpu_reserve: number
  cpu_max_per_container: number
}

export function fairCores(hostCores: number, containers: number, limits: CpuLimits): number {
  const usable = Math.max(1, hostCores - limits.cpu_reserve)
  const share = usable / Math.max(1, containers)
  return Math.floor(Math.min(limits.cpu_max_per_container, Math.max(0.25, share)) * 20) / 20
}
