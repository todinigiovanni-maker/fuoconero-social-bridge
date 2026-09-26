# Fuoconero Remote Reel Maker 0.4.0

WordPress owns durable render jobs, identities, nonces, existing encrypted Drive OAuth and output storage. Render executes FFmpeg one job at a time with a 10-minute renewable lease. Rendering is entirely separate from social publishing. The worker never invokes prepare or confirm.

## Client REST

Base: https://fuoconero-social-bridge.onrender.com

- POST /reel-maker/render-jobs
- GET /reel-maker/render-jobs/{render_job_id}
- GET /reel-maker/render-jobs/{render_job_id}/output
- GET /reel-maker/presets
- GET /reel-maker/article/{post_id}

The same render endpoints exist directly at https://fuoconero.com/wp-json/fuoconero-social/v2.

All requests except /health require existing WordPress HMAC. Four headers: X-FNS-Key, X-FNS-Timestamp (Unix seconds), X-FNS-Nonce (32–64 lowercase hex), X-FNS-Signature (64 lowercase hex). Canonical six fields joined by LF, no trailing LF:

    https://fuoconero.com
    METHOD
    /fuoconero-social/v2/reel-maker/...
    TIMESTAMP
    NONCE
    SHA256(raw body)

Sign with HMAC-SHA256 using the existing hex-decoded client secret. GET has an empty body. No query strings. The bridge preserves raw bytes/signature and forwards them to WordPress; it never substitutes its identity for a caller. WordPress enforces ±300 seconds, durable one-use nonces, 30 requests/minute and ownership. Existing max-render credentials/scopes remain unchanged; no keys are included in this repository.

Create example (replace source ID and category based on explicit selection):

```json
{"request_id":"unique-render-request-001","post_id":7443,"category":"poesie","music_title":"Fuoconero","outputs":{"reel":{"preset":"poesia","scene_texts":["Hook scelto dall’utente","Concetto essenziale"],"allow_extended_duration":false},"story":{"scene_texts":["Hook Story","Scopri l’articolo"],"allow_extended_duration":false}}}
```

Use music_title matching a unique library title exactly (case/whitespace normalized), or music_id. “Fuoconero” and “Fuoconero (rock)” never match each other. An ambiguous title is an error. Optional image_ids must refer to featured/content images belonging to the chosen post; no arbitrary asset URLs. Scene text is not generated from the whole article. It is supplied/edited by the caller. Overflow is paginated without losing words; durations longer than the preset guide require explicit allow_extended_duration=true. Resource ceiling is 600 seconds and 32 MiB per output; the renderer reports an error rather than truncating.

Creation returns render_job_id and status queued. Poll every 30–60 seconds. States: queued, rendering, uploading, completed, failed. Each output independently contains status, drive_file_id, storage_id, preview_url, SHA256, size, width, height, duration and render_metadata. Some outputs can complete before another fails. Every uploaded output creates a new file via unchanged FNS_Storage::upload; no overwriting.

Idempotent creation uses existing request_id + raw-body digest + owner/route. Poll the returned ID for current status. State survives Render cold starts/redeploys in WordPress. A rendering lease can be recovered (maximum three attempts); an uncertain upload is never retried automatically. Successful earlier outputs are retained. Expired/failed upload requires checking Drive, not submitting the same upload again.

Render Free can sleep/restart. A create request wakes /health; a request to the bridge wakes the service; the worker polls while awake. Cold starts may take about a minute. This is not an always-on latency guarantee. There is no new paid service or external queue.

Font rendering uses bundled dependency DejaVu Sans/Serif as deterministic fallback for configured font families, no remote font fetches. TTS and voiceover remain disabled.

## Worker-only REST

POST /reel-maker/render-worker/claim and POST /reel-maker/render-worker/{id}/{heartbeat|output|fail} require the existing max-render HMAC identity. Lease is server-generated and is never returned to normal status clients. Worker output upload reuses existing Drive OAuth only inside WordPress. No social credentials are copied to Render.

## Publication boundary

Completed rendering is not prepared/approved/queued for publishing. Storage references may be passed to the existing prepare flow later by the same owner. Explicit separate human digest confirmation remains mandatory. No new HTTP confirm endpoint is added to the bridge. Existing command-based publication code is unchanged; command.json remains noop during development. No real social publish is part of these tests.

An MCP/Actions tool catalog must expose these authenticated operations to make them selectable as native ChatGPT tools; this REST implementation alone does not add tools to an already connected MCP catalog.
