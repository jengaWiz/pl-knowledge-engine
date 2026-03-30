"""
YouTube channel metadata for target podcasts.
Each entry maps a channel to its YouTube channel ID and a description of its coverage.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PodcastChannel:
    """Metadata for a YouTube podcast channel.

    Attributes:
        name: Human-readable podcast/channel name.
        youtube_channel_id: The UC... channel ID from YouTube.
        coverage: Description of what teams/topics this channel covers.
        search_keywords: Keywords used to filter relevant episodes.
    """

    name: str
    youtube_channel_id: str
    coverage: str
    search_keywords: list[str]


PODCAST_CHANNELS = [
    PodcastChannel(
        name="The Football Ramble",
        youtube_channel_id="UCDC5_m85KU8tM3b3cgH-NFQ",
        coverage="General Premier League analysis, covers all teams weekly",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The 2 Robbies - NBC Sports",
        youtube_channel_id="UCqZQlzSHbVJrwrn5XvzrzcA",
        coverage="Match-by-match PL analysis from former professionals",
        search_keywords=["Premier League", "review", "preview"],
    ),
    PodcastChannel(
        name="Sky Sports Football Podcast",
        youtube_channel_id="UCNAf1k0yIjyGu3k9BwAg3lg",
        coverage="Post-match analysis with Sky Sports pundits",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The Anfield Wrap",
        youtube_channel_id="UCc5C5dNupCMbyutNatfBujQ",
        coverage="Dedicated Liverpool FC podcast — deep tactical analysis",
        search_keywords=["Liverpool", "Anfield", "Premier League"],
    ),
    PodcastChannel(
        name="The Villa View",
        youtube_channel_id="UCzYRArKryksXxf__E1D4gpQ",
        coverage="Dedicated Aston Villa podcast (rebranded as 1874: The Aston Villa Channel)",
        search_keywords=["Aston Villa", "Villa", "Premier League"],
    ),
    PodcastChannel(
        name="Official FPL Podcast",
        youtube_channel_id="UCG5qGWdu8nIRZqJ_GgDwQ-w",
        coverage="Fantasy Premier League — player performance discussion",
        search_keywords=["FPL", "gameweek"],
    ),
]
