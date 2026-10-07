"""Canonical routing separates reverse fixtures and refuses unresolved requests."""

import pytest

from src.retrieval.routing import normalize, route


@pytest.fixture
def plan():
    entities = [
        {"entity_type": "Team", "name": name, "canonical_id": name}
        for name in ["Liverpool", "Bournemouth", "Aston Villa", "Chelsea"]
    ]
    entities += [
        {"entity_type": "Player", "name": name, "canonical_id": ident}
        for name, ident in [
            ("Lucas Digne", "digne"),
            ("Mohamed Salah", "salah"),
            ("Alex Smith", "smith1"),
            ("Joe Smith", "smith2"),
        ]
    ]
    entities += [
        {
            "entity_type": "Match",
            "canonical_id": ident,
            "home_team": home,
            "away_team": away,
            "date": day,
        }
        for ident, home, away, day in [
            ("home", "Liverpool", "Bournemouth", "2025-08-15"),
            ("away", "Bournemouth", "Liverpool", "2026-01-24"),
            ("villa", "Aston Villa", "Liverpool", "2026-05-15"),
        ]
    ]
    return {
        "nodes": [{"kind": "Entity", "props": {**row, "season": "2025-26"}} for row in entities]
    }


@pytest.mark.parametrize(
    "query,expected",
    [
        ("Liverpool home match against Bournemouth", "home"),
        ("Liverpool at home against Bournemouth", "home"),
        ("Liverpool away match against Bournemouth", "away"),
        ("Liverpool at Bournemouth", "away"),
        ("Bournemouth at Liverpool", "home"),
        ("Liverpool against Bournemouth on 2025-08-15", "home"),
        ("Villa home match against Liverpool", "villa"),
        ("LIVERPOOL home vs BOURNEMOUTH 2025/26", "home"),
        ("Liverpool home vs Bournemouth 2025–2026", "home"),
    ],
)
def test_fixture_roles_and_dates(plan, query, expected):
    result = route(query, "2025-26", plan)
    assert result["status"] == "resolved" and result["entity_id"] == expected
    assert result["kind"] == "fixture"


@pytest.mark.parametrize(
    "query,status",
    [
        ("Liverpool vs Bournemouth", "clarification"),
        ("Liverpool home away against Bournemouth", "clarification"),
        ("Liverpool home against Bournemouth home", "clarification"),
        ("Liverpool against Bournemouth 2025-02-30", "clarification"),
        ("Liverpool vs Bournemouth 2025-08-15 and 2026-01-24", "clarification"),
        ("Liverpool home against Bournemouth on 2026-01-24", "unavailable"),
        ("Liverpool vs Arsenal", "clarification"),
        ("Liverpool home vs Bournemouth in 2024-25", "clarification"),
        ("Liverpool home vs Bournemouth in 2024/2025", "clarification"),
        ("Predict Liverpool vs Bournemouth", "unsupported"),
        ("Why did Liverpool win at Bournemouth?", "unsupported"),
        ("Liverpool home but not Bournemouth", "unsupported"),
        ("Digne against Real Madrid", "unavailable"),
        ("Digne against Liverpool at home", "clarification"),
        ("Digne against Liverpool on 2025-08-15", "clarification"),
        ("Smith against Liverpool", "clarification"),
        ("Compare Salah and Digne", "clarification"),
    ],
)
def test_ambiguity_constraints_and_unsupported_intents(plan, query, status):
    assert route(query, "2025-26", plan)["status"] == status


def test_player_surname_and_club_alias(plan):
    result = route("Digne appearances against Villa", "2025-26", plan)
    assert result["entity_id"] == "digne" and result["opponent"] == "Aston Villa"
    assert route("Lucas Digne appearances", "2025-26", plan)["opponent"] == ""


def test_unavailable_player_season_has_no_fallback(plan):
    assert route("Digne vs Liverpool", "2024-25", plan)["status"] == "unavailable"


def test_ambiguous_fixture_lists_both_candidates(plan):
    result = route("Liverpool vs Bournemouth", "2025-26", plan)
    assert {row["canonical_id"] for row in result["candidates"]} == {"home", "away"}


def test_accent_and_boundary_normalization(plan):
    assert normalize("João, Gómez") == "joao gomez"
    assert route("Salahuddin against Liverpool", "2025-26", plan)["status"] == "clarification"


@pytest.mark.parametrize(
    "query,season", [("", "2025-26"), ("x" * 4001, "2025-26"), ("Liverpool", "")]
)
def test_invalid_query_contract(plan, query, season):
    with pytest.raises(ValueError):
        route(query, season, plan)


@pytest.mark.parametrize(
    "canonical,alias",
    [
        ("Tottenham Hotspur", "Spurs"),
        ("Wolverhampton Wanderers", "Wolves"),
        ("West Ham United", "West Ham"),
        ("Newcastle United", "Newcastle"),
        ("Manchester United", "Man Utd"),
        ("Manchester City", "Man City"),
    ],
)
def test_aliases_use_real_canonical_club_names(plan, canonical, alias):
    plan["nodes"] += [
        {
            "kind": "Entity",
            "props": {
                "entity_type": "Team",
                "name": canonical,
                "canonical_id": canonical,
                "season": "2025-26",
            },
        },
        {
            "kind": "Entity",
            "props": {
                "entity_type": "Match",
                "canonical_id": "aliased",
                "home_team": canonical,
                "away_team": "Liverpool",
                "date": "2025-10-01",
                "season": "2025-26",
            },
        },
    ]
    result = route(f"{alias} home match against Liverpool", "2025-26", plan)
    assert result["status"] == "resolved" and result["entity_id"] == "aliased"


def test_full_name_disambiguates_shared_surname(plan):
    result = route("Alex Smith appearances against Liverpool", "2025-26", plan)
    assert result["status"] == "resolved" and result["entity_id"] == "smith1"
