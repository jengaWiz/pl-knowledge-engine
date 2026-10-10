"""Accent-insensitive roster lookup with explicit, bounded typo suggestions."""

import re
import unicodedata
from difflib import SequenceMatcher


def normalize(value):
    text = unicodedata.normalize("NFKD", value.casefold())
    return " ".join(
        re.findall(r"[a-z0-9]+", "".join(c for c in text if not unicodedata.combining(c)))
    )


def resolve_player(value, players):
    text = normalize(value)
    if not text or len(value) > 120:
        return {"status": "missing", "suggestions": []}
    exact, partial, close = [], [], []
    for player in players:
        aliases = [normalize(player.get(key) or "") for key in ("name", "full_name")]
        aliases = [alias for alias in aliases if alias]
        tokens = set(" ".join(aliases).split())
        candidate = {key: player.get(key) or "" for key in ("id", "name", "full_name")}
        if value == player["id"] or text in aliases or (len(text) >= 2 and text in tokens):
            exact.append(candidate)
        elif len(text) >= 3 and any(text in alias for alias in aliases):
            partial.append(candidate)
        elif len(text) >= 4:
            # Token similarity applies only to single-word input: do not turn a
            # missing full name into a different player with a similar surname.
            options = aliases + (list(tokens) if len(text.split()) == 1 else [])
            score = max(
                (SequenceMatcher(None, text, alias).ratio() for alias in options), default=0
            )
            if score >= 0.74:
                close.append((score, candidate))

    def order(player):
        return player["full_name"], player["id"]

    if exact:
        choices = sorted(exact, key=order)
    elif partial:
        choices = sorted(partial, key=order)
    else:
        choices = [p for _, p in sorted(close, key=lambda pair: (-pair[0], order(pair[1])))]
        return {"status": "suggestions" if choices else "missing", "suggestions": choices[:5]}
    if len(choices) == 1:
        return {"status": "matched", "player": choices[0], "suggestions": []}
    return {"status": "ambiguous", "suggestions": choices[:5]}
