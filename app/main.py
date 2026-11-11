"""TTS en streaming basé sur Kokoro-82M : l'audio part dès que la première phrase est prête.

- WebSocket  /ws/tts                  : texte -> trames PCM int16 24 kHz (une par segment de phrase)
- HTTP       /v1/audio/speech/stream  : texte -> flux PCM brut (chunked)
- HTTP       /v1/audio/speech         : texte -> mp3/wav complet (compatible OpenAI)
- HTTP       /split                   : aperçu du découpage (positions des segments)
- HTTP       /v1/audio/encode         : wav (rendu avec effets par le navigateur) -> mp3
- Page web   /                        : démo qui joue l'audio au fil de l'eau et mesure la latence
"""
import asyncio
import json
import re
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, StreamingResponse
from kokoro import KPipeline
from misaki import espeak
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

SAMPLE_RATE = 24000
DEFAULT_VOICE = "ff_siwis"  # seule voix française de Kokoro (féminine)
STATIC = Path(__file__).parent / "static"

LANG_BY_PREFIX = {"a": "a", "b": "b", "e": "e", "f": "f", "h": "h", "i": "i", "p": "p"}

KNOWN_VOICES = [
    "ff_siwis",
    "af_heart", "af_bella", "af_nicole", "am_adam", "am_michael",
    "bf_emma", "bm_george",
    "ef_dora", "em_alex",
    "if_sara", "im_nicola",
    "pf_dora", "pm_alex",
]

FORMATS = {
    "mp3": ("audio/mpeg", ["-f", "mp3", "-b:a", "128k"]),
    "wav": ("audio/wav", ["-f", "wav"]),
}
MAX_ENCODE_BYTES = 150 * 1024 * 1024  # ~13 min de wav stéréo 48 kHz

_pipelines: Dict[str, KPipeline] = {}
_lock = threading.Lock()  # une inférence à la fois (CPU)


def get_pipeline(lang: str) -> KPipeline:
    if lang not in _pipelines:
        _pipelines[lang] = KPipeline(lang_code=lang)
    return _pipelines[lang]


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Charge le modèle français et fait une synthèse à blanc : la 1re vraie requête n'attend pas
    pipe = get_pipeline("f")
    for _r in pipe("Bonjour.", voice=DEFAULT_VOICE, split_pattern=None):
        pass
    foreign_g2p("en")  # charge spaCy maintenant : le 1er <lang="en"> n'attend pas
    yield


app = FastAPI(title="Kokoro TTS streaming", version="0.1.0", lifespan=lifespan)


# ---------- découpage du texte pour réduire la latence ----------
Span = Tuple[int, int]  # [début, fin) dans le texte d'origine (indices en points de code)
SENTENCE_END = re.compile(r"(?<=[.!?…])\s+|\n+")


def _strip(text: str, a: int, b: int) -> Optional[Span]:
    while a < b and text[a].isspace():
        a += 1
    while b > a and text[b - 1].isspace():
        b -= 1
    return (a, b) if a < b else None


def _cut_long(text: str, a: int, b: int, min_len: int, max_len: int) -> List[Span]:
    """Coupe un passage trop long à la ponctuation faible, sinon à l'espace, jamais au milieu d'un mot si possible."""
    out: List[Span] = []
    while b - a > max_len:
        seg = text[a:a + max_len]
        cut = max(seg.rfind(c) for c in ",;:")
        if cut < min_len:
            cut = seg.rfind(" ")
        if cut < min_len:
            cut = max_len - 1
        span = _strip(text, a, a + cut + 1)
        if span:
            out.append(span)
        a += cut + 1
    span = _strip(text, a, b)
    return out + [span] if span else out


def split_spans(text: str, separator: str = "", auto: bool = True, min_len: int = 25, max_len: int = 140) -> List[Span]:
    """Segments envoyés un par un au modèle. Phrases courtes en premier : la latence au premier son dépend du 1er segment.

    `separator` force une coupure (il est retiré du texte, donc sans effet sur la voix) ; les segments ne sont jamais
    fusionnés par-dessus. `auto=False` ne coupe qu'aux séparateurs (et au-delà de 500 caractères, par sécurité).
    """
    bounds, a = [], 0
    if separator:
        for m in re.finditer(re.escape(separator), text):
            bounds.append((a, m.start()))
            a = m.end()
    bounds.append((a, len(text)))

    spans: List[Span] = []
    for a, b in bounds:
        if not auto:
            spans += _cut_long(text, a, b, min_len, 500)
            continue
        pieces, start = [], a
        for m in SENTENCE_END.finditer(text, a, b):
            pieces += _cut_long(text, start, m.start(), min_len, max_len)
            start = m.end()
        pieces += _cut_long(text, start, b, min_len, max_len)
        merged: List[Span] = []  # fusionne les segments trop courts (prosodie moins hachée)
        for p in pieces:
            if merged and merged[-1][1] - merged[-1][0] < min_len:
                merged[-1] = (merged[-1][0], p[1])
            else:
                merged.append(p)
        spans += merged
    return spans


def resolve_voice(voice: str) -> str:
    lang = LANG_BY_PREFIX.get(voice[:1])
    if lang is None:
        raise HTTPException(400, f"Voix inconnue: {voice}")
    return lang


# ---------- passages dans une autre langue : <lang="en">Dungeon World</lang> ----------
LANG_TAG = re.compile(r"""<lang\s*=?\s*["']?([a-z]{2,3}(?:-[a-z]+)?)["']?\s*>|</lang\s*>""", re.I)
ESPEAK_ALIASES = {"en": "en-us", "fr": "fr-fr", "pt": "pt-br"}


def lang_runs(text: str, a: int, b: int) -> List[Tuple[str, Optional[str]]]:
    """Morceaux de text[a:b] sans balises, avec leur langue (None = langue de la voix)."""
    runs: List[Tuple[str, Optional[str]]] = []

    def add(x: int, y: int, lang: Optional[str]) -> None:
        x, y = max(x, a), min(y, b)
        if x < y and text[x:y].strip():
            runs.append((text[x:y], lang))

    lang, pos = None, 0
    for m in LANG_TAG.finditer(text):  # balises non imbriquées : </lang> revient à la langue de la voix
        add(pos, m.start(), lang)
        lang, pos = (m.group(1).lower() if m.group(1) else None), m.end()
    add(pos, len(text), lang)
    return runs


KOKORO_EN = {"en": "a", "en-us": "a", "en-gb": "b"}  # anglais : G2P natif de Kokoro (misaki), meilleur accent


@lru_cache(maxsize=None)
def foreign_g2p(lang: str):
    if lang in KOKORO_EN:
        return KPipeline(lang_code=KOKORO_EN[lang], repo_id="hexgrad/Kokoro-82M", model=False).g2p
    try:
        return espeak.EspeakG2P(language=ESPEAK_ALIASES.get(lang, lang))
    except RuntimeError:
        raise ValueError(f'Langue inconnue dans <lang="{lang}"> (codes espeak-ng : en, en-gb, es, it, de…)')


def synth_segments(text: str, voice: str, speed: float, cancel: threading.Event,
                   separator: str = "", auto: bool = True) -> Iterator[bytes]:
    """Générateur synchrone : un bloc PCM int16 par segment de texte."""
    pipe = get_pipeline(resolve_voice(voice))
    with _lock:
        for a, b in split_spans(text, separator, auto):
            if cancel.is_set():
                return
            runs = lang_runs(text, a, b)
            if not runs:
                continue
            if all(lang is None for _, lang in runs):
                audios = [r.audio for r in pipe(" ".join(t for t, _ in runs), voice=voice, speed=speed,
                                                split_pattern=None)]
            else:
                # Phonèmes de chaque langue mis bout à bout puis synthétisés d'un seul tenant : même voix, prosodie continue
                ps = " ".join(p for t, lang in runs if (p := (foreign_g2p(lang) if lang else pipe.g2p)(t)[0]))
                pack = pipe.load_voice(voice).to(pipe.model.device)
                audios = [KPipeline.infer(pipe.model, ps[:510], pack, speed).audio] if ps else []
            for audio in audios:
                if audio is None or cancel.is_set():
                    continue
                arr = audio.numpy() if hasattr(audio, "numpy") else np.asarray(audio)
                yield (np.clip(arr, -1, 1) * 32767).astype("<i2").tobytes()


# ---------- WebSocket ----------
@app.websocket("/ws/tts")
async def ws_tts(ws: WebSocket) -> None:
    await ws.accept()
    loop = asyncio.get_running_loop()
    try:
        while True:  # une connexion peut enchaîner plusieurs requêtes
            req = json.loads(await ws.receive_text())
            text = str(req.get("text", "")).strip()
            voice = str(req.get("voice", DEFAULT_VOICE))
            speed = min(max(float(req.get("speed", 1.0)), 0.5), 2.0)
            separator = str(req.get("separator", ""))[:5]
            auto = bool(req.get("auto_split", True))
            if not text or len(text) > 5000:
                await ws.send_text(json.dumps({"type": "error", "message": "texte vide ou > 5000 caractères"}))
                continue
            try:
                resolve_voice(voice)
            except HTTPException as exc:
                await ws.send_text(json.dumps({"type": "error", "message": exc.detail}))
                continue

            queue: asyncio.Queue = asyncio.Queue(maxsize=8)
            cancel = threading.Event()
            t0 = time.perf_counter()

            def producer() -> None:
                try:
                    for chunk in synth_segments(text, voice, speed, cancel, separator, auto):
                        asyncio.run_coroutine_threadsafe(queue.put(chunk), loop).result()
                except Exception as exc:  # erreur de synthèse
                    asyncio.run_coroutine_threadsafe(queue.put(exc), loop).result()
                finally:
                    asyncio.run_coroutine_threadsafe(queue.put(None), loop).result()

            threading.Thread(target=producer, daemon=True).start()
            await ws.send_text(json.dumps({"type": "start", "sample_rate": SAMPLE_RATE, "format": "pcm_s16le"}))
            n, ttfa = 0, None
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        break
                    if isinstance(item, Exception):
                        await ws.send_text(json.dumps({"type": "error", "message": str(item)}))
                        break
                    if ttfa is None:
                        ttfa = int((time.perf_counter() - t0) * 1000)
                    n += 1
                    await ws.send_bytes(item)
                await ws.send_text(json.dumps({"type": "done", "segments": n, "server_ttfa_ms": ttfa}))
            except WebSocketDisconnect:
                cancel.set()
                raise
    except WebSocketDisconnect:
        return


# ---------- HTTP ----------
class SpeechRequest(BaseModel):
    input: str = Field(..., min_length=1, max_length=5000)
    voice: str = Field(DEFAULT_VOICE)
    response_format: str = Field("mp3", description="mp3 ou wav")
    speed: float = Field(1.0, ge=0.5, le=2.0)
    separator: str = Field("", max_length=5, description="Coupure forcée, retirée du texte (ex: |)")
    auto_split: bool = Field(True, description="Découpage automatique en phrases")
    model: str = Field("kokoro", description="Ignoré (compatibilité OpenAI)")


@app.post("/v1/audio/speech/stream")
def speech_stream(req: SpeechRequest) -> StreamingResponse:
    """Flux PCM brut s16le 24 kHz mono. Ex: curl -N ... | ffplay -f s16le -ar 24000 -ac 1 -"""
    resolve_voice(req.voice)
    cancel = threading.Event()
    return StreamingResponse(
        synth_segments(req.input, req.voice, req.speed, cancel, req.separator, req.auto_split),
        media_type="audio/L16;rate=24000;channels=1",
        headers={"X-Sample-Rate": str(SAMPLE_RATE), "Cache-Control": "no-store"},
    )


@app.post("/v1/audio/speech")
def speech(req: SpeechRequest) -> Response:
    fmt = req.response_format.lower()
    if fmt not in FORMATS:
        raise HTTPException(400, f"response_format doit être parmi {list(FORMATS)}")
    pcm = b"".join(synth_segments(req.input, req.voice, req.speed, threading.Event(), req.separator, req.auto_split))
    if not pcm:
        raise HTTPException(422, "Aucun audio généré")
    return ffmpeg_encode(["-f", "s16le", "-ar", str(SAMPLE_RATE), "-ac", "1"], pcm, fmt)


@app.post("/v1/audio/encode")
async def encode(request: Request) -> Response:
    """Corps : fichier wav (ou tout format lisible par ffmpeg). Réponse : mp3."""
    data = await request.body()
    if not data:
        raise HTTPException(400, "Corps vide")
    if len(data) > MAX_ENCODE_BYTES:
        raise HTTPException(413, "Fichier trop volumineux")
    return await run_in_threadpool(ffmpeg_encode, [], data, "mp3")


def ffmpeg_encode(input_args: List[str], data: bytes, fmt: str) -> Response:
    proc = subprocess.run(
        ["ffmpeg", "-loglevel", "error", *input_args, "-i", "pipe:0", *FORMATS[fmt][1], "pipe:1"],
        input=data, capture_output=True,
    )
    if proc.returncode != 0:
        raise HTTPException(500, f"Erreur d'encodage ffmpeg: {proc.stderr.decode(errors='replace')}")
    return Response(content=proc.stdout, media_type=FORMATS[fmt][0])


class SplitRequest(BaseModel):
    text: str = Field(..., max_length=5000)
    separator: str = Field("", max_length=5)
    auto_split: bool = True


@app.post("/split")
def split(req: SplitRequest) -> dict:
    """Aperçu du découpage : positions [début, fin) en points de code dans le texte."""
    return {"segments": split_spans(req.text, req.separator, req.auto_split)}


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/voices")
def voices() -> dict:
    return {"default": DEFAULT_VOICE, "voices": KNOWN_VOICES}


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")
