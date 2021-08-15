from dataclasses import dataclass, field
from typing import List


@dataclass(frozen=True)
class search:
    languages: 'list[str]' = field(default_factory=list)
    games: 'list[str]' = field(default_factory=list)
    limit: int = 100
