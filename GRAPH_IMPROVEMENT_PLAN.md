# Neo4j Graph — Full Diagnostic & Improvement Plan

## Current State (Verified)

**Nodes:** 202 total across 6 types
**Relationships:** 275 total — no isolated (unconnected) nodes

| Node Type | Count | Connected? |
|-----------|-------|------------|
| Player    | 85    | Yes → Team (PLAYS_FOR) |
| Match     | 74    | Yes → Team (HOME_TEAM/AWAY_TEAM), Gameweek (PART_OF) |
| Gameweek  | 38    | Yes → Season (PART_OF) |
| Team      | 2     | Yes → Stadium (PLAYS_AT), Match |
| Stadium   | 2     | Yes → Team |
| Season    | 1     | Yes → Gameweek |

| Relationship   | Count | Direction |
|----------------|-------|-----------|
| PART_OF        | 112   | Match→Gameweek (74) + Gameweek→Season (38) |
| PLAYS_FOR      | 85    | Player→Team |
| HOME_TEAM      | 38    | Team→Match |
| AWAY_TEAM      | 38    | Team→Match |
| PLAYS_AT       | 2     | Team→Stadium |

---

## Why the Graph Looks "Disconnected"

The relationships technically exist, but the graph has a **structural hub problem**:

```
Season ← Gameweek ← Match ← Team → Player
                              ↓
                           Stadium
```

- **Players dead-end at Team.** There is no path from a Player to a Match.
- **Team is the only hub.** 85 Players + 74 Matches + 2 Stadiums all terminate at just 2 Team nodes.
- **No cross-entity edges.** You cannot ask "which players appeared in this match?" or "what did a player do in GW1?" — the data exists in CSVs but is not in the graph.
- **PodcastEpisodes don't exist in the graph at all** — never loaded.
- **Team has no Season link** — Teams float without explicit season membership.
- **Match.gameweek is not a stored property** — only reachable by traversing PART_OF to Gameweek node.

---

## The Data We Already Have (Unused)

### GW Player Stats CSVs (`data/raw/stats/fpl/GW1/` … `GW31/`)
Each file has **~70 rows × 34 columns** including:
```
player_id, fixture_id, was_home, minutes, goals_scored, assists,
clean_sheets, goals_conceded, yellow_cards, red_cards, saves, bonus,
bps, total_points, expected_goals, expected_assists, starts, value
```
This directly links **players to matches with performance stats** — 31 GW files × ~70 rows = ~2,170 player-match records sitting unused.

### Season-Level Player Stats (`data/raw/stats/fpl/players.csv`)
106 columns per player including:
```
goals_scored, assists, clean_sheets, minutes, yellow_cards, red_cards,
expected_goals, expected_assists, influence, creativity, threat, ict_index,
starts, form, points_per_game, total_points, now_cost
```
These should be stored as **Player node properties**.

---

## Fix Plan (Ordered by Impact)

---

### Fix 1 — Add `PlayerAppearance` Nodes (HIGHEST IMPACT)

**Problem:** No path exists between Player and Match.
**Solution:** Create `PlayerAppearance` nodes — one per player per GW — linking Players to Matches with performance data.

**New node type:**
```
PlayerAppearance {
  appearance_id:   "{player_id}_{fixture_id}"  ← unique key
  minutes:         int
  goals_scored:    int
  assists:         int
  clean_sheets:    int
  goals_conceded:  int
  yellow_cards:    int
  red_cards:       int
  saves:           int
  bonus:           int
  bps:             int
  total_points:    int
  expected_goals:  float
  expected_assists: float
  starts:          bool
  was_home:        bool
}
```

**New relationships:**
```
(p:Player)-[:HAD_APPEARANCE]->(a:PlayerAppearance)-[:IN_MATCH]->(m:Match)
```

**Data source:** `data/raw/stats/fpl/GW{n}/playerstats_gw.csv`
**Key join:** `fixture_id` in GW stats → `id` in matches CSV → `match_id` in Neo4j

**Result:** Graph goes from a hub-spoke to a full mesh:
```
Player → PlayerAppearance → Match → Gameweek → Season
  ↓                           ↓
Team                        Team
  ↓
Stadium
```

**New queries unlocked:**
```cypher
// Top scorers for Aston Villa
MATCH (p:Player)-[:PLAYS_FOR]->(t:Team {name:"Aston Villa"})
MATCH (p)-[:HAD_APPEARANCE]->(a:PlayerAppearance)
RETURN p.first_name + " " + p.last_name AS player, sum(a.goals_scored) AS goals
ORDER BY goals DESC LIMIT 10

// Players who appeared in a specific match
MATCH (a:PlayerAppearance)-[:IN_MATCH]->(m:Match {match_id:"2"})
MATCH (p:Player)-[:HAD_APPEARANCE]->(a)
RETURN p.web_name, a.minutes, a.goals_scored, a.assists

// Head-to-head stats
MATCH (t1:Team {name:"Aston Villa"})-[:HOME_TEAM|AWAY_TEAM]->(m:Match)
MATCH (t2:Team {name:"Liverpool"})-[:HOME_TEAM|AWAY_TEAM]->(m)
MATCH (p:Player)-[:HAD_APPEARANCE]->(a:PlayerAppearance)-[:IN_MATCH]->(m)
RETURN m.date, p.web_name, a.goals_scored, a.assists
```

**Implementation:**
1. Add `create_player_appearances(gw_stats_dir)` to `Neo4jStore`
2. Add constraint: `PlayerAppearance.appearance_id IS UNIQUE`
3. Load all GW CSVs in `load_graph.py` after `create_matches()`

---

### Fix 2 — Enrich Player Nodes with Season Stats

**Problem:** Player nodes have only name and position; the full 106-column FPL CSV is unused.
**Solution:** Add key season-level stats as Player node properties.

**New Player properties:**
```
goals_scored, assists, clean_sheets, minutes, yellow_cards, red_cards,
expected_goals, expected_assists, form, total_points, now_cost,
points_per_game, starts, influence, creativity, threat, ict_index
```

**Implementation:**
Add `SET p.goals_scored = $goals_scored, p.assists = $assists, ...` in `_merge_player()` in `Neo4jStore`.
These come from `players.csv` which is already loaded in `_load_rosters()` in `load_graph.py`.

---

### Fix 3 — Add `Team -[:IN_SEASON]-> Season`

**Problem:** Team nodes have no link to Season — they float without temporal context.
**Solution:** Extend `create_teams()` to add a Season link.

```cypher
MERGE (t:Team {name: $name})
SET t.abbreviation = $abbreviation, t.stadium = $stadium, t.city = $city
WITH t
MATCH (s:Season {label: $season})
MERGE (t)-[:IN_SEASON]->(s)
```

**Why it matters:**
```cypher
// All teams in 2025-26
MATCH (t:Team)-[:IN_SEASON]->(s:Season {label:"2025-26"})
RETURN t.name

// Full season subgraph
MATCH (s:Season {label:"2025-26"})
MATCH (t:Team)-[:IN_SEASON]->(s)
MATCH (t)-[:PLAYS_AT]->(st:Stadium)
RETURN s, t, st
```

---

### Fix 4 — Store `Match.gameweek` as Property

**Problem:** `m.gameweek` returns `None` — you must traverse PART_OF to know a match's gameweek.
**Solution:** Add `gameweek` to the `SET` in `create_matches()`.

```cypher
MERGE (m:Match {match_id: $match_id})
SET m.date = $date,
    m.home_score = $home_score,
    m.away_score = $away_score,
    m.home_team_name = $home_team,
    m.away_team_name = $away_team,
    m.gameweek = $gameweek        ← ADD THIS
```

---

### Fix 5 — Load PodcastEpisodes into Graph

**Problem:** `create_podcast_episode()` method exists in `Neo4jStore` but is never called.
**Solution:** Call it from `load_graph.py` and add relationships to Season.

**New relationship:**
```
(e:PodcastEpisode)-[:COVERS_SEASON]->(s:Season)
```

**Future relationships (when transcript pipeline is complete):**
```
(e:PodcastEpisode)-[:MENTIONS]->(p:Player)
(e:PodcastEpisode)-[:DISCUSSES]->(m:Match)
```

---

## Summary of New Node/Relationship Count After Fixes

| Fix | New Nodes | New Relationships |
|-----|-----------|-------------------|
| 1: PlayerAppearance | ~2,170 nodes | ~4,340 rels (HAD_APPEARANCE + IN_MATCH) |
| 2: Player stats enrichment | 0 (property update) | 0 (property update) |
| 3: Team→Season | 0 | 2 rels |
| 4: Match.gameweek property | 0 (property update) | 0 |
| 5: PodcastEpisode loading | N episodes | N rels |

**Before:** 202 nodes, 275 relationships
**After Fix 1-4:** ~2,372 nodes, ~4,617 relationships

---

## Implementation Order

1. **Fix 4** — trivial one-liner, unblocks clean queries immediately
2. **Fix 3** — 3 lines of Cypher, fixes Team isolation
3. **Fix 2** — extend `_merge_player()` with season stats columns
4. **Fix 1** — new `create_player_appearances()` method + new node type + seed constraint (biggest change, most impact)
5. **Fix 5** — after podcast episode data exists

---

## Files to Modify

| File | Changes Needed |
|------|---------------|
| `src/store/neo4j_store.py` | Fix 1 (new method), Fix 2 (extend `_merge_player`), Fix 3 (extend `create_teams`), Fix 4 (extend `create_matches`) |
| `scripts/load_graph.py` | Fix 1 (call new method), Fix 2 (pass stats columns), Fix 5 (call episode loader) |
| `scripts/seed_graph.py` | Fix 1 (add `PlayerAppearance.appearance_id` constraint) |
