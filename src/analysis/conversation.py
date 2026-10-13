"""Grounded conversational answers from reviewed match and appearance records.

A local model may classify wording, but never supplies facts or source references.
"""

import json
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.analysis import deductions
from src.clean.corpus_quality import load_verified_corpus
from src.retrieval.routing import ALIASES, normalize

KINDS = {"team_stats", "form", "home_away", "player_rankings", "player", "fixture"}
CAUSAL = re.compile(
    r"\b(why|cause\w*|because|tactic\w*|injur\w*|predict\w*|forecast\w*|will|would|should|odds|bet\w*)\b",
    re.I,
)
SEASON = re.compile(r"\b(20\d{2})\s*[-/–]\s*(20\d{2}|\d{2})(?![-/]\d)")
DATE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")


def _season(text):
    found = SEASON.findall(text)
    return f"{found[0][0]}-{found[0][1][-2:]}" if found else ""


def _mentions(text, teams):
    normalized = normalize(text)
    hits = []
    for team in teams:
        aliases = [team, *ALIASES.get(team, [])]
        if any(
            re.search(r"(?<!\w)" + re.escape(normalize(a)) + r"(?!\w)", normalized) for a in aliases
        ):
            hits.append(team)
    return hits


def _intent(text):
    q = normalize(text)
    if re.search(r"\b(last five|last 5|recent form|form)\b", q):
        return "form"
    if re.search(r"\b(home|away)\b", q) and not re.search(
        r"\b(home match|away match|at home against)\b", q
    ):
        return "home_away"
    if re.search(
        r"\b(top scorers|most goals|most assists|top players|player rankings|per 90|most minutes)\b",
        q,
    ):
        return "player_rankings"
    if re.search(r"\b(score|scored|won|winner|result|fixture|match|versus|vs)\b", q):
        return "fixture"
    if re.search(
        r"\b(points|goals|goal difference|standings|record|stats|statistics|performed)\b", q
    ):
        return "team_stats"
    return ""


def _model_plan(message, history, teams, model, url):
    if not model:
        return {}
    system = (
        "Classify the latest football question only. Return JSON with kind and teams. "
        "kind must be one of team_stats, form, home_away, player_rankings, player, fixture, unsupported. "
        "teams must contain exact names from this list only: " + ", ".join(teams) + ". "
        "Use earlier user messages only to resolve a clear follow-up. "
        "For causal/tactical/prediction questions choose unsupported. Never answer the question."
    )
    context = [
        {"role": "user", "content": h["content"][:250]} for h in history if h["role"] == "user"
    ][-3:]
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            *context,
            {"role": "user", "content": message[:1000]},
        ],
        "format": "json",
        "stream": False,
        "options": {"temperature": 0, "num_predict": 100},
        "keep_alive": "5m",
    }
    try:
        request = Request(
            url.rstrip("/") + "/api/chat",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=35) as response:
            raw = json.loads(response.read(4096))
        plan = json.loads(raw["message"]["content"])
        if not isinstance(plan, dict) or plan.get("kind") not in KINDS | {"unsupported"}:
            return {}
        chosen = plan.get("teams", [])
        if not isinstance(chosen, list) or len(chosen) > 2 or any(t not in teams for t in chosen):
            return {}
        return {"kind": plan["kind"], "teams": chosen}
    except (HTTPError, URLError, TimeoutError, ValueError, KeyError, TypeError, OSError):
        return {}


def _unavailable(season, reply, *, suggestions=None):
    return {
        "status": "insufficient_evidence",
        "season": season,
        "reply": reply,
        "sources": [],
        "suggestions": suggestions or [],
    }


def _source(row):
    ref = row.get("source") or {
        "url": row["source_url"],
        "row": row["source_row"],
        "sha256": row.get("source_sha256", ""),
    }
    return {
        "type": "match",
        "url": ref["url"],
        "summary": f"Original result · row {ref['row']}",
        "record_ids": [row["id"]],
        "source_rows": [ref["row"]],
        "source_sha256": ref.get("sha256", ""),
    }


def _fixture(matches, teams, text, season):
    if len(teams) != 2:
        return _unavailable(
            season, "Name two clubs for a match question, for example Liverpool vs Bournemouth."
        )
    candidates = [m for m in matches if {m["home_team"], m["away_team"]} == set(teams)]
    date = DATE.findall(text)
    if date:
        candidates = [m for m in candidates if m["date"] == date[0]]
    q = normalize(text)
    if "home" in q or "away" in q:
        for team in teams:
            if normalize(team) + " home" in q or "home " + normalize(team) in q:
                candidates = [m for m in candidates if m["home_team"] == team]
            if normalize(team) + " away" in q or "away " + normalize(team) in q:
                candidates = [m for m in candidates if m["away_team"] == team]
    if not candidates:
        return _unavailable(season, "I couldn't find that fixture in the verified season records.")
    if len(candidates) > 1:
        return _unavailable(
            season,
            "I found both home and away fixtures. Which date do you mean?",
            suggestions=[
                f"{m['home_team']} vs {m['away_team']} on {m['date']}" for m in candidates
            ],
        )
    m = candidates[0]
    winner = (
        m["home_team"]
        if m["home_score"] > m["away_score"]
        else m["away_team"]
        if m["away_score"] > m["home_score"]
        else "a draw"
    )
    return {
        "status": "ok",
        "season": season,
        "operation": "fixture",
        "teams": teams,
        "reply": f"On **{m['date']}**, {m['home_team']} beat {m['away_team']} **{m['home_score']}–{m['away_score']}**."
        if winner == m["home_team"]
        else f"On **{m['date']}**, {m['away_team']} beat {m['home_team']} **{m['away_score']}–{m['home_score']}**."
        if winner == m["away_team"]
        else f"On **{m['date']}**, {m['home_team']} and {m['away_team']} drew **{m['home_score']}–{m['away_score']}**.",
        "sources": [_source(m)],
        "suggestions": [],
    }


def _team_result(output, season, matches, teams, kind):
    if not teams:
        return _unavailable(season, "Which club should I look up?")
    if season == "2025-26":
        result = deductions.analyze(output, season, kind, teams=teams)
        rows, sources = result["rows"], result["sources"]
    else:
        rows = []
        for team in teams:
            games = deductions.result_rows(matches, team)
            if kind == "form":
                recent = games[-5:]
                rows.append(
                    {
                        "team": team,
                        **deductions.totals(recent),
                        "form": "".join(g["result"] for g in recent),
                    }
                )
            elif kind == "home_away":
                rows.append(
                    {
                        "team": team,
                        "home": deductions.totals([g for g in games if g["home"]]),
                        "away": deductions.totals([g for g in games if not g["home"]]),
                    }
                )
            else:
                rows.append({"team": team, **deductions.totals(games)})
        sources = [
            _source(m) for m in matches if m["home_team"] in teams or m["away_team"] in teams
        ]
    lines = [f"**{season} season · {kind.replace('_', ' ')}**"]
    for row in rows:
        if kind == "home_away":
            h, a = row["home"], row["away"]
            lines.append(
                f"{row['team']} earned **{h['points']} points in {h['matches']} home matches** and **{a['points']} points in {a['matches']} away matches**."
            )
        else:
            suffix = (
                f" Their last-five form was **{row['form']}** (oldest to newest)."
                if kind == "form"
                else ""
            )
            lines.append(
                f"{row['team']}: **{row['points']} points**, {row['wins']} wins, {row['draws']} draws, {row['losses']} losses and a **{row['goal_difference']:+} goal difference** across {row['matches']} matches.{suffix}"
            )
    lines.append(
        "These are recorded league results; they don't establish causes or forecast future matches."
    )
    return {
        "status": "ok",
        "season": season,
        "operation": kind,
        "teams": teams,
        "reply": "\n\n".join(lines),
        "sources": sources[:5],
        "suggestions": [],
    }


def _player_result(output, season, text, corpus):
    if season != "2025-26":
        return _unavailable(
            season,
            "Player appearances are available for the 2025–26 Aston Villa and Liverpool snapshot. Choose that season to ask about a player.",
        )
    q = normalize(text)
    people = [
        p
        for p in corpus["players"]
        if any(
            re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", q)
            for name in {
                normalize(p["name"]),
                normalize(p["web_name"]),
                normalize(p["name"].split()[-1]),
            }
            if len(name) >= 4
        )
    ]
    if len(people) != 1:
        return _unavailable(
            season, "Name one player from the stored Aston Villa or Liverpool roster."
        )
    person = people[0]
    apps = [a for a in corpus["appearances"] if a["player_id"] == person["id"]]
    if not apps:
        return _unavailable(
            season, "I found that player, but no verified appearances in this snapshot."
        )
    goals = sum(a["goals"] for a in apps if a["goals"] is not None)
    assists_known = all(a["assists"] is not None for a in apps)
    assists = sum(a["assists"] for a in apps if a["assists"] is not None)
    minutes = sum(a["minutes"] for a in apps)
    latest = sorted(apps, key=lambda a: (a["date"], a["id"]), reverse=True)[:5]
    return {
        "status": "ok",
        "season": season,
        "operation": "player",
        "teams": sorted({a["team"] for a in apps}),
        "reply": f"**{person['name']}** has **{goals} goals**"
        + (f", **{assists} assists**" if assists_known else "; assist coverage is incomplete")
        + f" and **{minutes} recorded minutes** across {len(apps)} stored appearances for {', '.join(sorted({a['team'] for a in apps}))}. This covers the verified 2025–26 snapshot, not every club.",
        "sources": [
            {
                "type": "appearance",
                "url": a["source"]["url"],
                "summary": f"Appearance on {a['date']} · row {a['source']['row']}",
                "record_ids": [a["id"]],
            }
            for a in latest
        ],
        "suggestions": [],
    }


def converse(
    output, season, message, history, *, model="", model_url="http://host.docker.internal:11434"
):
    """Resolve a question using bounded context and verified rows only."""
    from src.prediction.sources import load_results

    if CAUSAL.search(message):
        return _unavailable(
            season,
            "I cannot establish that from these records. I can't establish causes, tactics, injuries or future outcomes from these records. I can show recorded results, form, players and sources.",
        )
    requested = _season(message)
    if requested:
        season = requested
    if season not in {"2022-23", "2023-24", "2024-25", "2025-26"}:
        return _unavailable(season, "Choose a stored season from 2022–23 through 2025–26.")
    # Keep the established direct-answer behavior and source contracts for current-season metrics.
    if season == "2025-26" and not re.search(
        r"\b(score|scored|won|winner|result|fixture|match|versus|vs)\b", normalize(message)
    ):
        direct = deductions.answer(output, season, message)
        if direct["status"] == "ok":
            return {**direct, "suggestions": []}
    corpus = load_verified_corpus(output, "2025-26") if season == "2025-26" else None
    matches = (
        corpus["matches"]
        if corpus
        else [m for m in load_results(output)[0] if m["season"] == season]
    )
    all_teams = sorted({m[k] for m in matches for k in ("home_team", "away_team")})
    explicit = _mentions(message, all_teams)
    last_teams, last_kind = [], ""
    for item in reversed(history):
        if item["role"] != "user":
            continue
        if not last_teams:
            last_teams = _mentions(item["content"], all_teams)
        if not last_kind:
            last_kind = _intent(item["content"])
        if last_teams and last_kind:
            break
    kind = _intent(message)
    teams = explicit or last_teams
    if not kind:
        kind = last_kind
    if (
        not explicit
        and len(teams) > 1
        and re.search(r"\b(they|them|their|that team|what about)\b", normalize(message))
    ):
        return _unavailable(
            season, "Which club do you mean?", suggestions=[f"{t} points" for t in teams]
        )
    if not explicit or not kind:
        plan = _model_plan(message, history, all_teams, model, model_url)
        if plan.get("kind") == "unsupported":
            return _unavailable(
                season,
                "I can't verify that kind of answer from the stored football records. Try a club, player, match, form or home/away question.",
            )
        if not kind:
            kind = plan.get("kind", "")
        if not teams:
            teams = plan.get("teams", [])
    if kind == "fixture":
        fixture_text = message
        if not DATE.search(message):
            fixture_text += " " + next(
                (
                    h["content"]
                    for h in reversed(history)
                    if h["role"] == "user" and DATE.search(h["content"])
                ),
                "",
            )
        return _fixture(matches, teams, fixture_text, season)
    if kind == "player" or (
        not kind
        and corpus
        and any(normalize(p["web_name"]) in normalize(message) for p in corpus["players"])
    ):
        return _player_result(output, season, message, corpus or {})
    if kind == "player_rankings":
        if season != "2025-26":
            return _unavailable(
                season,
                "Player rankings are available for the 2025–26 Aston Villa and Liverpool snapshot.",
            )
        metric = (
            "assists"
            if "assist" in normalize(message)
            else "minutes"
            if "minute" in normalize(message)
            else "goals"
        )
        if not explicit and not re.search(r"\b(assist|goal|minute)\b", normalize(message)):
            for item in reversed(history):
                if item["role"] == "user" and re.search(
                    r"\b(assist|goal|minute)\b", normalize(item["content"])
                ):
                    metric = (
                        "assists"
                        if "assist" in normalize(item["content"])
                        else "minutes"
                        if "minute" in normalize(item["content"])
                        else "goals"
                    )
                    break
        result = deductions.analyze(
            output,
            season,
            kind,
            teams=teams or ["Liverpool", "Aston Villa"],
            metric=metric,
            limit=5,
        )
        lines = [f"**{season} · top {metric}**"] + [
            f"{i + 1}. {row['name']}: **{row['value']} {metric}** in {row['appearances']} appearances for {', '.join(row['teams'])}."
            for i, row in enumerate(result["rows"])
        ]
        return {
            "status": "ok",
            "season": season,
            "operation": kind,
            "teams": teams,
            "reply": "\n\n".join(lines)
            + "\n\nOnly the stored Aston Villa and Liverpool appearances are covered.",
            "sources": result["sources"][:5],
            "suggestions": [],
        }
    if kind in {"team_stats", "form", "home_away"}:
        return _team_result(output, season, matches, teams, kind)
    return _unavailable(
        season,
        "I can answer questions about recorded club form, points, players and match results. Name a club or fixture to get started.",
    )
