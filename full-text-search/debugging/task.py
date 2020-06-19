from typing import List
import json

JSON_STRING = """
{
  "title": "Звездные войны 1: Империя приносит баги",
  "description": "Эпичная сага по поиску багов в старом легаси проекте Империи",
  "tags": [
    2, "семейное кино", "космос", 1.0, "сага", "эпик", "добро против зла", true,
    "челмедведосвин", "debug", "ipdb", "PyCharm", "боевик", "боевик", "эникей","дарт багус",
    5, 6,4, "блокбастер", "кино 2020", 7, 3, 9, 12, "каникулы в космосе", "коварство"
  ],
  "version": 17
}
"""


def unique_tags(payload: dict) -> List[str]:
    tags = payload.get('tags', [])
    for i, tag in enumerate(tags):
        if type(tag) in (bool, float):
            tags[i] = str(tag)

    result = list(set(tags))
    return result


if __name__ == '__main__':
    unique_tags(json.loads(JSON_STRING))
