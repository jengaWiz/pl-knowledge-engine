"""Conservative deterministic routes; unresolved identities never widen retrieval."""

import re
import unicodedata
from datetime import date

ALIASES = {
    "Aston Villa": ["Villa"],
    "Manchester City": ["Man City"],
    "Manchester United": ["Man United", "Man Utd"],
    "Tottenham Hotspur": ["Tottenham", "Spurs"],
    "Wolverhampton Wanderers": ["Wolverhampton", "Wolves"],
    "Nottingham Forest": ["Nott'm Forest", "Nottm Forest", "Forest"],
    "Newcastle United": ["Newcastle"],
    "West Ham United": ["West Ham"],
    "Brighton": ["Brighton and Hove Albion"],
    "Leeds": ["Leeds United"],
    "Bournemouth": ["AFC Bournemouth"],
}


def normalize(value):
    value = unicodedata.normalize("NFKD", value.casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.findall(r"[a-z0-9]+", value))


def mentions(text, entities, *, players=False):
    found = []
    for entity in entities:
        names = [entity["name"]]
        names += [entity["name"].split()[-1]] if players else ALIASES.get(entity["name"], [])
        spans = []
        for name in names:
            alias = normalize(name)
            if len(alias) >= 3:
                spans.extend(
                    (m.start(), m.end())
                    for m in re.finditer(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", text)
                )
        if spans:
            # Prefer the full name to a surname/short alias inside it.
            start, end = max(spans, key=lambda pair: pair[1] - pair[0])
            found.append({"entity": entity, "start": start, "end": end})
    # A full name disambiguates a surname shared by another player.
    found = [
        item
        for item in found
        if not any(
            other["start"] <= item["start"]
            and other["end"] >= item["end"]
            and other["end"] - other["start"] > item["end"] - item["start"]
            for other in found
        )
    ]
    return sorted(found, key=lambda item: (item["start"], item["end"]))


def route(query, evidence_season, plan):
    if not isinstance(query, str) or not query.strip() or len(query) > 4000:
        raise ValueError("Query must contain 1–4000 characters")
    if not evidence_season:
        raise ValueError("An explicit evidence season is required")
    base = {"season": evidence_season}

    def result(status, reason, **fields):
        return {**base, "status": status, "reason": reason, **fields}

    available = [
        row["props"]
        for row in plan["nodes"]
        if row["kind"] == "Entity" and row["props"]["season"] == evidence_season
    ]
    if not available:
        return result("unavailable", "The requested season has no accepted evidence.")
    seasons = re.findall(r"\b(20\d{2})\s*[-/–]\s*(20\d{2}|\d{2})(?![\d-])", query)
    if any(f"{year}-{end[-2:]}" != evidence_season for year, end in seasons):
        return result("clarification", "The query season conflicts with the selected season.")
    text = normalize(query)
    if re.search(
        r"\b(predict\w*|forecast\w*|will|tomorrow|tactic\w*|why|cause\w*|not|except)\b", text
    ):
        return result("unsupported", "This route supports recorded fixtures and appearances only.")
    dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", query)
    try:
        for item in dates:
            date.fromisoformat(item)
    except ValueError:
        return result("clarification", "Use a valid ISO fixture date (YYYY-MM-DD).")
    if len(set(dates)) > 1:
        return result("clarification", "Choose one fixture date.")
    teams = mentions(text, [e for e in available if e["entity_type"] == "Team"])
    players = mentions(text, [e for e in available if e["entity_type"] == "Player"], players=True)
    if players:
        if len(players) != 1 or len(teams) > 1 or dates or re.search(r"\b(home|away)\b", text):
            return result(
                "clarification",
                "Choose one player and at most one opponent; omit dates and home/away constraints.",
            )
        if not teams and re.search(r"\b(against|versus|vs)\b", text):
            return result("unavailable", "The named opponent is not available in this season.")
        return result(
            "resolved",
            "Canonical player appearance route.",
            kind="player",
            entity_id=players[0]["entity"]["canonical_id"],
            opponent=teams[0]["entity"]["name"] if teams else "",
        )
    if len(teams) != 2:
        return result("clarification", "Name two clubs and specify home/away or an ISO date.")
    names = {item["entity"]["name"] for item in teams}
    candidates = [
        e
        for e in available
        if e["entity_type"] == "Match" and {e["home_team"], e["away_team"]} == names
    ]
    home, away = set(), set()
    for index, mention in enumerate(teams):
        stop = teams[index + 1]["start"] if index + 1 < len(teams) else len(text)
        following = text[mention["end"] : stop]
        if re.search(r"\bhome\b", following):
            home.add(mention["entity"]["name"])
        if re.search(r"\baway\b", following):
            away.add(mention["entity"]["name"])
    # "Liverpool at Bournemouth" identifies the visitor without assuming vs order.
    between = text[teams[0]["end"] : teams[1]["start"]].strip()
    if between == "at":
        away.add(teams[0]["entity"]["name"])
        home.add(teams[1]["entity"]["name"])
    if len(home) > 1 or len(away) > 1 or home & away:
        return result("clarification", "Home and away instructions conflict.")
    if home:
        candidates = [e for e in candidates if e["home_team"] in home]
    if away:
        candidates = [e for e in candidates if e["away_team"] in away]
    if dates:
        candidates = [e for e in candidates if e["date"] == dates[0]]
    if not candidates:
        return result("unavailable", "No accepted fixture matches these constraints.")
    if len(candidates) != 1:
        return result(
            "clarification",
            "Specify the home club, away club or ISO fixture date.",
            candidates=[
                {key: e[key] for key in ("canonical_id", "date", "home_team", "away_team")}
                for e in sorted(candidates, key=lambda e: e["date"])
            ],
        )
    return result(
        "resolved",
        "Canonical fixture route.",
        kind="fixture",
        entity_id=candidates[0]["canonical_id"],
        opponent="",
    )
