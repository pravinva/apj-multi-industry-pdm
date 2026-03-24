"""
Genie space setup helpers.
"""

from pathlib import Path
import json


def load_genie_questions(industry: str) -> list[str]:
    path = Path("industries") / industry / "genie_questions.json"
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))
