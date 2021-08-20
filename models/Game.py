from datetime import datetime
from py2neo.ogm import Model, Property, RelatedFrom, RelatedTo


class Game(Model):
    """Game title object model"""
    __primarylabel__ = "Game"
    __primarykey__ = "title"
    title = Property()
    year = Property()
    videos = RelatedFrom("Video", "OF_GAME")

    def __init__(self, title: str, year: int):
        self.title = title
        self.year = year
