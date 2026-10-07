"""Stable descriptions of a full match record; absent metrics remain unknown."""


def render_match_text(match: dict) -> str:
    season = match["season"]

    def known(value):
        return "unknown" if value is None else str(value)

    return (
        f"{season} Premier League, {match['date']}: {match['home_team']} "
        f"{match['home_score']}–{match['away_score']} {match['away_team']}. "
        f"Shots: {known(match['home_shots'])}–{known(match['away_shots'])}; "
        f"shots on target: {known(match['home_shots_on_target'])}–"
        f"{known(match['away_shots_on_target'])}."
    )
