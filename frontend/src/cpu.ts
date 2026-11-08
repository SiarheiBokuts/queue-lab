/**
 * How many cores every sender and worker container gets. The same rule as `fair_cpu` in backend/app/budget.py (keep in
 * sync): the cores of Docker minus one kept for Kafka and the app, shared equally by all containers, at least a
 * quarter of a core and at most four, rounded down to a twentieth of a core.
 */
export const CPU_RESERVE = 1
export const CPU_MAX_PER_CONTAINER = 4

export function fairCores(hostCores: number, containers: number): number {
  const usable = Math.max(1, hostCores - CPU_RESERVE)
  const share = usable / Math.max(1, containers)
  return Math.floor(Math.min(CPU_MAX_PER_CONTAINER, Math.max(0.25, share)) * 20) / 20
}
