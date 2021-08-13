from dataclasses import dataclass, field


@dataclass(frozen=True)
class search:
    languages: list[str] = field(default_factory=list)
    games: list[str] = field(default_factory=list)
    limit: int = 100
