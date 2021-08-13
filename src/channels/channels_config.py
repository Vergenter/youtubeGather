from dataclasses import dataclass


@dataclass(frozen=True)
class channels:
    update_frequency: int = 0
    get_videos: bool = False
    latest_videos_percent: float = 0
