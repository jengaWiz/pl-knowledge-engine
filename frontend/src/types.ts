export interface GraphNode {
  id: string
  name: string
  type: 'Season' | 'Team' | 'Stadium' | 'Player' | 'Gameweek' | 'Match' | 'PlayerAppearance'
  // optional type-specific props
  position?: string
  goals?: number
  assists?: number
  team?: string
  home?: string
  away?: string
  gw?: number
  number?: number
  home_score?: number
  away_score?: number
  date?: string
  total_points?: number
  minutes?: number
  abbreviation?: string
}

export interface GraphEdge {
  source: string
  target: string
  type: string
}

export interface GraphData {
  nodes: GraphNode[]
  edges: GraphEdge[]
}

export interface Match {
  id: string
  date: string
  home_team: string
  away_team: string
  home_score: number | null
  away_score: number | null
  gameweek: number
}

export interface TopPlayer {
  web_name: string
  position: string
  value: number
  team: string
}

export interface ChatMessage {
  role: 'user' | 'assistant'
  content: string
  sources?: { type: string; summary: string }[]
}
