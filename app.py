"""Paradise Tunes V8: ISOLATED, experimental analyzer. Never deploy over V1.
Derived from the owner's V1 reference analyzer; live stream is read-only.
One Gunicorn worker only: each worker would open its own stream connection.
"""
import json
import math
import os
import subprocess
import threading
import time
from collections import deque

import numpy as np
from flask import Flask, Response, jsonify, stream_with_context

STREAM_URL = os.getenv('PARADISE_STREAM_URL', 'https://s2.free-shoutcast.com/stream/18102')
RATE = 11025
FRAME = 1024
BANDS = 24
MIN_HZ = 45.0
MAX_HZ = 5000.0
HOP = int(os.getenv('VU_HOP', '512'))
ALPHA = float(os.getenv('VU_ALPHA', '0.85'))
if not 1 <= HOP <= FRAME:
    raise ValueError('VU_HOP must be between 1 and 1024')
if not 0.0 < ALPHA <= 1.0:
    raise ValueError('VU_ALPHA must be within (0,1]')

app = Flask(__name__)
_lock = threading.Lock()
_new_frame = threading.Condition(_lock)
_state = dict(seq=0, bands=[0]*BANDS, raw_bands=[0]*BANDS,
              generated_epoch=None, generated_mono=None,
              live_status='starting', error=None, frame_interval_ms=None,
              recent_rate_hz=None, max_recent_gap_ms=None,
              connection_id=0)
_started = False
_edges = np.geomspace(MIN_HZ, MAX_HZ, BANDS+1)
_freqs = np.fft.rfftfreq(FRAME, d=1.0/RATE)
_masks = [(_freqs >= lo) & (_freqs < hi) for lo,hi in zip(_edges[:-1],_edges[1:])]
_window = np.hanning(FRAME)


def frequency_bands(audio):
    """Faithful V1 frequency range, bins and dB-to-0..100 mapping."""
    spectrum = np.abs(np.fft.rfft(audio*_window))
    vals = []
    for use in _masks:
        magnitude = float(np.sqrt(np.mean(spectrum[use]**2))) if np.any(use) else 0.0
        db = 20.0*math.log10(max(magnitude, 1e-9))
        vals.append(int(round(max(0.0,min(100.0,(db+22)*1.6)))))
    return np.asarray(vals, dtype=np.float64)


def consume_stream():
    delays = (2,4,8,15,30)
    attempt = 0
    while True:
        proc = None
        try:
            with _lock:
                _state.update(live_status='connecting', error=None)
                _state['connection_id'] += 1
                connection_id = _state['connection_id']
            proc = subprocess.Popen([
                'ffmpeg','-hide_banner','-nostdin','-loglevel','error',
                '-rw_timeout','12000000','-i',STREAM_URL,
                '-vn','-ac','1','-ar',str(RATE),'-f','f32le','pipe:1'
            ], stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,bufsize=0)
            attempt = 0
            # Assemble the initial 1024-sample window. Subsequent windows
            # consume HOP new samples, retaining FRAME-HOP older samples.
            buffer = np.zeros(FRAME,dtype=np.float64)
            filled = 0
            smooth = np.zeros(BANDS,dtype=np.float64)
            recent = deque(maxlen=50)
            previous_mono = None
            while True:
                needed = FRAME-filled if filled < FRAME else HOP
                target = needed*4
                chunks = bytearray()
                while len(chunks) < target:
                    chunk = proc.stdout.read(target-len(chunks))
                    if not chunk:
                        raise ConnectionError('Stream ended or FFmpeg stopped')
                    chunks.extend(chunk)
                incoming = np.frombuffer(chunks,dtype='<f4').astype(np.float64)
                if filled < FRAME:
                    buffer[filled:filled+needed] = incoming
                    filled += needed
                else:
                    buffer[:-HOP] = buffer[HOP:]
                    buffer[-HOP:] = incoming
                raw = frequency_bands(buffer)
                smooth = ALPHA*raw+(1-ALPHA)*smooth
                now_mono = time.monotonic()
                now_epoch = time.time()
                interval_ms = ((now_mono-previous_mono)*1000.0
                               if previous_mono is not None else None)
                previous_mono = now_mono
                recent.append(now_mono)
                rate = ((len(recent)-1)/(recent[-1]-recent[0])
                        if len(recent)>1 and recent[-1]>recent[0] else None)
                with _new_frame:
                    _state['seq'] += 1
                    _state.update(
                        bands=[int(round(x)) for x in smooth],
                        raw_bands=[int(x) for x in raw],
                        generated_epoch=now_epoch, generated_mono=now_mono,
                        frame_interval_ms=round(interval_ms,2) if interval_ms is not None else None,
                        recent_rate_hz=round(rate,2) if rate is not None else None,
                        max_recent_gap_ms=(round(max([(b-a)*1000 for a,b in zip(list(recent)[:-1],list(recent)[1:])]),2)
                                           if len(recent)>1 else None),
                        live_status='live',error=None,connection_id=connection_id)
                    _new_frame.notify_all()
        except Exception as exc:
            with _lock:
                _state.update(live_status='reconnecting',error=str(exc)[:160],
                              bands=[0]*BANDS,raw_bands=[0]*BANDS,
                              generated_epoch=None,generated_mono=None)
        finally:
            if proc is not None:
                try:
                    proc.kill()
                    proc.wait(timeout=3)
                except Exception:
                    pass
        time.sleep(delays[min(attempt,len(delays)-1)])
        attempt += 1


def start_worker():
    global _started
    with _lock:
        if _started:
            return
        _started=True
    threading.Thread(target=consume_stream,daemon=True,name='v8-fft-reader').start()


def snapshot():
    with _lock:
        data = dict(_state)
        data['bands'] = list(_state['bands'])
        data['raw_bands'] = list(_state['raw_bands'])
    age = time.monotonic()-data['generated_mono'] if data['generated_mono'] is not None else None
    live = age is not None and 0 <= age < 3
    return dict(
        version=8,source='real-audio-fft',experimental=True,
        band_count=BANDS,live=live,status=data['live_status'] if live else 'waiting',
        seq=data['seq'],connection_id=data['connection_id'],
        generated_at_unix_ms=(round(data['generated_epoch']*1000)
                              if data['generated_epoch'] is not None else None),
        sample_age_ms=(round(age*1000,1) if age is not None else None),
        frame_interval_ms=data['frame_interval_ms'],
        recent_rate_hz=data['recent_rate_hz'],
        max_recent_gap_ms=data['max_recent_gap_ms'],
        audio_window_ms=round(FRAME/RATE*1000,2),
        nominal_hop_ms=round(HOP/RATE*1000,2),
        smoothing_alpha=ALPHA,
        bands=data['bands'] if live else [0]*BANDS,
        raw_bands=data['raw_bands'] if live else [0]*BANDS,
        error=data['error'] if not live else None,
    )


@app.get('/api/events')
def events():
    # One persistent SSE connection per viewing browser, served by a Gunicorn thread.
    # No extra FFmpeg input: events share the one analyzer state/stream.
    # The condition variable wakes listeners when the FFT thread produces a frame.
    def generate():
        last = -1
        yield 'retry: 2500\n\n'
        while True:
            with _new_frame:
                _new_frame.wait_for(lambda: _state['seq'] != last, timeout=10)
                current = _state['seq']
            if current == last:
                yield ': keepalive\n\n'
                continue
            last = current
            data = snapshot()
            yield 'data: ' + json.dumps(data, separators=(',', ':')) + '\n\n'

    return Response(stream_with_context(generate()), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache, no-transform',
        'X-Accel-Buffering': 'no',
        'Access-Control-Allow-Origin': '*',
        'Connection': 'keep-alive',
    })


@app.get('/health')
def health():
    return jsonify(ok=True,service='paradise-tunes-v8-sse-isolated',experimental=True)


@app.get('/api/vu')
def vu():
    return jsonify(snapshot()), 200, {'Cache-Control':'no-store','Access-Control-Allow-Origin':'*'}


@app.get('/')
def home():
    with open(os.path.join(os.path.dirname(__file__),'index.html'),encoding='utf-8') as f:
        page=f.read()
    return Response(page,mimetype='text/html')


start_worker()
