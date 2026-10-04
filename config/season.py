"""Season evidence and team mapping for the live FPL adapters."""

from datetime import date, datetime
from typing import Any


def season_bounds(season: str) -> tuple[date, date]:
    """Use July-to-June bounds, including delayed fixtures within a season."""
    try:
        start, end = season.split("-")
        year = int(start)
        if len(start) != 4 or len(end) != 2 or int(end) != (year + 1) % 100:
            raise ValueError
        return date(year, 7, 1), date(year + 1, 7, 1)
    except (ValueError, TypeError) as exc:
        raise ValueError("Season must be consecutive years, for example 2025-26") from exc


def validate_season_dates(values: list[str], season: str) -> None:
    """Fail closed when a dataset has missing or out-of-season date evidence."""
    lower, upper = season_bounds(season)
    if not values:
        raise ValueError("Cannot establish the source season: no dated records")
    for value in values:
        try:
            day = datetime.fromisoformat(value.replace("Z", "+00:00")).date()
        except (ValueError, AttributeError) as exc:
            raise ValueError("Cannot establish the source season: invalid date") from exc
        if not lower <= day < upper:
            raise ValueError(f"Source date {day} does not belong to configured season {season}")


def validate_bootstrap(bootstrap: dict[str, Any], season: str) -> None:
    validate_season_dates(
        [event.get("deadline_time", "") for event in bootstrap.get("events", [])], season
    )


def resolve_focus_ids(teams: list[dict[str, Any]], names: set[str]) -> dict[int, str]:
    """Resolve season-specific IDs from names; never assume last season's IDs."""
    result = {}
    for name in sorted(names):
        matches = [team for team in teams if team.get("name") == name]
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one source team named {name}")
        team_id = int(matches[0]["id"])
        if team_id in result:
            raise ValueError("Source assigns the same ID to different focus teams")
        result[team_id] = name
    return result
