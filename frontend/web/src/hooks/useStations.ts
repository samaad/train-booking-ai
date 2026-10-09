import { useQuery } from '@tanstack/react-query'
import { api } from '../api/endpoints'

export function useStations() {
  const query = useQuery({ queryKey: ['stations'], queryFn: api.stations, staleTime: 60 * 60_000 })
  const byCode = new Map((query.data ?? []).map((s) => [s.code, s]))
  const name = (code: string) => byCode.get(code)?.name ?? code
  return { ...query, name }
}
