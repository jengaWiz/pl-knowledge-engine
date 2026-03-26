"""
YouTube channel metadata for target podcasts.
Each entry maps a channel to its YouTube channel ID and a description of its coverage.

To find a channel ID:
1. Go to the channel page on YouTube
2. View page source or use YouTube Data API: channels.list with forUsername parameter
3. Or use a tool like https://commentpicker.com/youtube-channel-id.php
"""
from dataclasses import dataclass, field


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
    youtube_channel_id: str  # The UC... ID from YouTube
    coverage: str  # What teams/topics this channel covers
    search_keywords: list[str]  # Keywords to filter relevant episodes


PODCAST_CHANNELS = [
    PodcastChannel(
        name="The Football Ramble",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="General Premier League analysis, covers all teams weekly",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The 2 Robbies - NBC Sports",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Match-by-match PL analysis from former professionals",
        search_keywords=["Premier League", "review", "preview"],
    ),
    PodcastChannel(
        name="Sky Sports Football Podcast",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Post-match analysis with Sky Sports pundits",
        search_keywords=["Premier League", "Aston Villa", "Liverpool"],
    ),
    PodcastChannel(
        name="The Anfield Wrap",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Dedicated Liverpool FC podcast — deep tactical analysis",
        search_keywords=["Liverpool", "Anfield", "Premier League"],
    ),
    PodcastChannel(
        name="The Villa View",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Dedicated Aston Villa podcast",
        search_keywords=["Aston Villa", "Villa", "Premier League"],
    ),
    PodcastChannel(
        name="Official FPL Podcast",
        youtube_channel_id="",  # TODO: look up channel ID
        coverage="Fantasy Premier League — player performance discussion",
        search_keywords=["FPL", "gameweek"],
    ),
]
