from dataclasses import dataclass


@dataclass(frozen=True)
class videos:
    update_frequency: int = 0
    get_comments: bool = False
