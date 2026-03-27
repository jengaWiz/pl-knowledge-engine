"""
Team metadata for the two focus teams.
All team-specific constants live here — player lists, API IDs, YouTube channels, etc.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class TeamConfig:
    """Configuration for a Premier League team.

    Attributes:
        name: Full team name (e.g., "Aston Villa").
        short_name: Shortened team name (e.g., "Villa").
        abbreviation: Three-letter abbreviation (e.g., "AVL").
        balldontlie_id: Numeric ID from the BallDontLie EPL API.
        fbref_id: FBref unique team identifier string.
        stadium: Name of the team's home stadium.
        stadium_city: City where the stadium is located.
    """

    name: str
    short_name: str
    abbreviation: str
    balldontlie_id: int  # Look up from BallDontLie API on first run
    fbref_id: str
    stadium: str
    stadium_city: str


# These IDs need to be verified against the BallDontLie API on first run.
# Run: GET https://api.balldontlie.io/epl/v2/teams and find the correct IDs.
ASTON_VILLA = TeamConfig(
    name="Aston Villa",
    short_name="Villa",
    abbreviation="AVL",
    balldontlie_id=3,  # Verified via BallDontLie EPL v2 API docs
    fbref_id="8602292d",
    stadium="Villa Park",
    stadium_city="Birmingham",
)

LIVERPOOL = TeamConfig(
    name="Liverpool",
    short_name="Liverpool",
    abbreviation="LFC",
    balldontlie_id=12,  # Verified via BallDontLie EPL v2 API docs
    fbref_id="822bd0ba",
    stadium="Anfield",
    stadium_city="Liverpool",
)

FOCUS_TEAMS = [ASTON_VILLA, LIVERPOOL]
FOCUS_TEAM_NAMES = {t.name for t in FOCUS_TEAMS}
