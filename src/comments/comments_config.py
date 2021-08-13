from dataclasses import dataclass


@dataclass(frozen=True)
class comments:
    update_frequency: int = 0
    get_replies: bool = False
