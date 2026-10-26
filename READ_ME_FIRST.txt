PARADISE TUNES V8 — FINAL CONTINUOUS DELIVERY EXPERIMENT
========================================================

PURPOSE
Compare one continuous SSE connection against repeated /api/vu HTTP polling,
on the SAME independent V8 cloud FFT analyzer. Avoid changing V1 or V7.

SAFETY
- Use an entirely NEW public GitHub repository, e.g. paradise-tunes-vu-analyzer-v8.
- Use a new, separate Render Web Service, e.g. paradise-tunes-vu-analyzer-v8.
- DO NOT upload V8 files over V1 or V7 and DO NOT redeploy existing services.
- Use Docker, branch main, Oregon region, root directory blank, FREE ($0/month)
  only, Auto-Deploy Off, health path /health, no env vars/secret files.
- Only proceed if Render shows $0 per month. Free instances can sleep after idle.
- Do not upload to Bluehost or install on either Toshiba.
- One service process: FFmpeg opens one independent, read-only SHOUTcast connection.
- SSE adds a persistent HTTP client connection; each viewer occupies a request thread.
- This prototype is NOT designed for public listener traffic.

REPOSITORY FILES TO UPLOAD
app.py, Dockerfile, requirements.txt, index.html, READ_ME_FIRST.txt
Optionally upload .dockerignore. It is included but Docker can build without it.

TEST
1. Once Render shows successful deployment, open /health. Expect experimental
   true, ok true, service 'paradise-tunes-v8-sse-isolated'.
2. Open the V8 service homepage '/' on Windows Chrome and let it run 30–45s.
3. Record the SSE changed seq/sec, polling changed seq/sec, analyzer frames/sec,
   average arrival spacing, longest gap, errors, and whether bars hesitate.
4. Do not confuse the count of skipped IDs with error: intermediate FFT frames
   are often skipped due to network/proxy scheduling.
5. If SSE stream is interrupted or buffered, note that instead of claiming success.
6. If delivery improves, test on iPhone Safari separately. Smooth movement alone
   is not a success: cloud audio and listener playback are separate buffered streams.

STOP / GO DECISION
GO ONLY IF SSE consistently delivers substantially more frames AND visibly smoother
music-responsive motion, AND synchronized music tests are acceptable.
STOP cloud-VU experimentation if hesitation persists or actual audio synchronization
remains poor. Recommend a different iPhone architecture, with honest labeling of
visualizations; never pretend decorative animation is an actual synchronized VU.

TECHNICAL
24 FFT bands; 11025 Hz; 1024 sample window; 512 sample hop; alpha .85.
SSE endpoint /api/events sends text/event-stream notifications from one background
FFT worker using threading.Condition, with 10-second keepalive and retry hints.
HTTP /api/vu is retained as V8 diagnostic reference. One Gunicorn worker, 8
threads. Render's reverse proxy may buffer, suspend, or interrupt connections.
All experimental paths are standalone and do not require production site changes.
