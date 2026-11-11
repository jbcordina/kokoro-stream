"""Lancer : docker exec kokoro-stream python -m app.test_split"""
from app.main import lang_runs, split_spans


def segs(text, **kw):
    return [text[a:b] for a, b in split_spans(text, **kw)]


assert segs("Bonjour ! Ceci est une phrase assez longue. Et une autre.") == \
    ["Bonjour ! Ceci est une phrase assez longue.", "Et une autre."]
long = "mot " * 60  # 240 caractères sans ponctuation : coupe à l'espace, jamais dans un mot
assert all(not s.startswith("ot") and len(s) <= 140 for s in segs(long))
assert segs("Un. | Deux trois quatre cinq six.", separator="|") == ["Un.", "Deux trois quatre cinq six."]
assert segs("Une phrase. Une autre phrase.|Fin", separator="|", auto=False) == ["Une phrase. Une autre phrase.", "Fin"]
assert segs("a||b", separator="|") == ["a", "b"]
assert segs("   ") == []
t = 'Bienvenue dans <lang="en">Dungeon World : Earth</lang>, aventurier.'
assert lang_runs(t, 0, len(t)) == [("Bienvenue dans ", None), ("Dungeon World : Earth", "en"), (", aventurier.", None)]
t = "<lang=en>Hello. World</lang> fin"
assert [r for a, b in split_spans(t, min_len=1) for r in lang_runs(t, a, b)] == [("Hello.", "en"), ("World", "en"), (" fin", None)]
print("ok")
