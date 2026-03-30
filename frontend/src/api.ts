import axios from 'axios'
import type { GraphData, Match, TopPlayer, ChatMessage } from './types'

const api = axios.create({ baseURL: '/api' })

export async function fetchOverviewGraph(): Promise<GraphData> {
  const { data } = await api.get('/graph/overview')
  return data
}

export async function fetchPlayerGraph(webName: string): Promise<GraphData> {
  const { data } = await api.get(`/graph/player/${encodeURIComponent(webName)}`)
  return data
}

export async function fetchMatchGraph(matchId: string): Promise<GraphData> {
  const { data } = await api.get(`/graph/match/${matchId}`)
  return data
}

export async function fetchMatches(): Promise<Match[]> {
  const { data } = await api.get('/matches')
  return data
}

export async function fetchTopPlayers(
  team = '',
  stat = 'goals_scored',
  limit = 10,
): Promise<TopPlayer[]> {
  const { data } = await api.get('/stats/top-players', {
    params: { team, stat, limit },
  })
  return data
}

export async function sendChat(
  message: string,
  history: ChatMessage[],
): Promise<{ reply: string; sources: { type: string; summary: string }[] }> {
  const { data } = await api.post('/chat', {
    message,
    history: history.map(m => ({ role: m.role, content: m.content })),
  })
  return data
}
