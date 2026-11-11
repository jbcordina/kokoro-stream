# kokoro-stream

TTS en streaming (Kokoro-82M, Apache 2.0) : l'audio démarre dès que la première phrase est synthétisée.

```bash
make start                    # docker compose up --build -d, puis affiche l'URL de la page web
docker compose logs -f        # attendre "Application startup complete" (téléchargement ~330 Mo au 1er lancement)
make stop                     # docker compose down
```

## Démo web

Ouvre http://localhost:8003 : saisis un texte, clique « Lire ». La page affiche le délai avant le premier son.
Les réglages d'effets peuvent être enregistrés comme préréglages perso (champ « Nom du préréglage », boutons « Enregistrer » / « Supprimer »), stockés dans le navigateur.
Après une lecture, « MP3 voix brute » télécharge la sortie Kokoro telle quelle ; « MP3 avec effets » rejoue l'audio avec les effets actuels dans le navigateur (`OfflineAudioContext`) puis l'encode via `POST /v1/audio/encode` (wav -> mp3, ffmpeg).

## Mesurer la latence avec curl

```bash
# Temps avant le premier octet audio (time_starttransfer)
curl -s -o /dev/null -w "1er octet: %{time_starttransfer}s  total: %{time_total}s\n" \
  -X POST http://localhost:8003/v1/audio/speech/stream -H "Content-Type: application/json" \
  -d '{"input":"Bonjour ! Ceci est un test. Voici une deuxième phrase.","voice":"ff_siwis"}'

# Écouter au fil de l'eau (ffplay requis, ex: brew install ffmpeg)
curl -sN -X POST http://localhost:8003/v1/audio/speech/stream -H "Content-Type: application/json" \
  -d '{"input":"Bonjour ! Ceci est un test. Voici une deuxième phrase.","voice":"ff_siwis"}' \
  | ffplay -autoexit -nodisp -f s16le -ar 24000 -ac 1 -

# mp3 complet (non streaming)
curl -X POST http://localhost:8003/v1/audio/speech -H "Content-Type: application/json" \
  -d '{"input":"Bonjour.","voice":"ff_siwis"}' --output out.mp3
```

## Protocole WebSocket `/ws/tts`

- Client : `{"text": "...", "voice": "ff_siwis", "speed": 1.0, "separator": "|", "auto_split": true}` (`separator` force une coupure et est retiré du texte ; `auto_split: false` ne coupe qu'aux séparateurs). Aperçu : `POST /split` renvoie les positions des segments.
- Passage dans une autre langue : `<lang="en">Dungeon World : Earth</lang>` (codes espeak-ng : `en`, `en-gb`, `es`, `it`, `de`…). La voix reste la même, seule la prononciation change. L'anglais (`en`, `en-gb`) utilise le G2P natif de Kokoro, les autres langues espeak-ng.
- Serveur : `{"type":"start","sample_rate":24000,"format":"pcm_s16le"}`, puis des trames **binaires** PCM int16 mono (une par segment), puis `{"type":"done","segments":N,"server_ttfa_ms":...}`.
- On peut enchaîner plusieurs requêtes sur la même connexion ; fermer la connexion annule la synthèse en cours.

Côté navigateur : décoder chaque trame en Float32 et la planifier avec Web Audio (`AudioBufferSourceNode.start(t)`) — voir `app/static/index.html`.

## Pourquoi c'est réactif

Le texte est découpé en phrases courtes (segments d'au moins 25 caractères, coupés à la virgule au-delà de 140). La latence au premier son est donc celle de la **première phrase seulement**, pas du texte entier. Le modèle est préchauffé au démarrage.

## Limites

- Une seule voix française (`ff_siwis`, féminine), pas de clonage ni de réglage d'émotion.
- Les requêtes sont traitées une à la fois (verrou), ce qui suffit pour un usage perso.
- Un petit silence peut apparaître entre deux segments si le CPU génère plus lentement que la lecture.
