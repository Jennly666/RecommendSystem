from dataclasses import dataclass
from typing import List

@dataclass
class Video:
    id: str
    genres: List[str]

@dataclass
class Interaction:
    user_id: str
    video_id: str
    action: str