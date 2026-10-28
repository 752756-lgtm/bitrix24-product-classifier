from __future__ import annotations

import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .core import AI, Bitrix, Catalog, Config, PickerError, Pipeline, RetryLater, identifier, render


class Queue:
    def __init__(self, path):
        self.path = path
        Path(path).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with self.db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, dedupe TEXT UNIQUE NOT NULL, deal_id INTEGER NOT NULL,
                activity_id INTEGER NOT NULL, write_mode INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
                due REAL NOT NULL DEFAULT 0, result TEXT, error TEXT,
                created REAL NOT NULL, updated REAL NOT NULL)""")
            db.execute("CREATE UNIQUE INDEX IF NOT EXISTS marker_write ON jobs(json_extract(result, '$.marker')) WHERE write_mode=1 AND result IS NOT NULL")
        os.chmod(path, 0o600)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def enqueue(self, deal_id, activity_id, write_mode):
        key = f"{deal_id}:{activity_id}:{int(write_mode)}"
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT id, state FROM jobs WHERE dedupe=?", (key,)).fetchone()
            if old:
                return dict(old)
            count = db.execute("SELECT count(*) FROM jobs WHERE state IN ('queued','processing','writing')").fetchone()[0]
            if count >= 1000:
                raise RetryLater("queue_full")
            job_id = uuid.uuid4().hex
            now = time.time()
            db.execute("INSERT INTO jobs (id,dedupe,deal_id,activity_id,write_mode,created,updated) VALUES (?,?,?,?,?,?,?)",
                       (job_id, key, deal_id, activity_id, int(write_mode), now, now))
            return {"id": job_id, "state": "queued"}

    def recover(self):
        # Exactly one worker process owns this volume; Docker deployment stops it before replacement.
        with self.db() as db:
            db.execute("UPDATE jobs SET state='queued' WHERE state='processing'")
            db.execute("UPDATE jobs SET state='uncertain',error='write_interrupted' WHERE state='writing'")

    def claim(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE state='queued' AND due<=? ORDER BY created LIMIT 1", (time.time(),)).fetchone()
            if not row:
                return None
            db.execute("UPDATE jobs SET state='processing',attempts=attempts+1,updated=? WHERE id=?", (time.time(), row['id']))
            result = dict(row)
            result['attempts'] += 1
            return result

    def finish(self, job_id, state, error=None, result=None, due=0):
        with self.db() as db:
            db.execute("UPDATE jobs SET state=?,error=?,result=COALESCE(?,result),due=?,updated=? WHERE id=?",
                (state, error, None if result is None else json.dumps(result, ensure_ascii=False), due, time.time(), job_id))

    def get(self, job_id):
        with self.db() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return dict(row) if row else None


class Worker:
    def __init__(self, config, queue, pipeline):
        self.config, self.queue, self.pipeline = config, queue, pipeline

    def run_once(self):
        job = self.queue.claim()
        if not job:
            return False
        writing = False
        try:
            result = self.pipeline.prepare(job['deal_id'], job['activity_id'])
            self.queue.finish(job['id'], 'processing', result=result)
            if not job['write_mode']:
                self.queue.finish(job['id'], 'dry_run_done')
                return True
            if not self.config.write_enabled:
                self.queue.finish(job['id'], 'blocked', 'write_disabled')
                return True
            if self.pipeline.bitrix.comment_exists(job['deal_id'], result['marker']):
                self.queue.finish(job['id'], 'done')
                return True
            comment = render(result)
            self.pipeline.validate_source(result)
            # Persist the uncertainty boundary BEFORE the only external mutation.
            self.queue.finish(job['id'], 'writing')
            writing = True
            self.pipeline.bitrix.add_comment(job['deal_id'], comment)
            if not self.pipeline.bitrix.comment_exists(job['deal_id'], result['marker']):
                raise PickerError('write_not_verified')
            self.queue.finish(job['id'], 'done')
        except sqlite3.IntegrityError:
            # Another request already prepared the same email in write mode.
            self.queue.finish(job['id'], 'duplicate', 'email_already_registered')
        except RetryLater as exc:
            if writing:
                self.queue.finish(job['id'], 'uncertain', 'write_outcome_unknown')
            elif job['attempts'] < 6:
                self.queue.finish(job['id'], 'queued', str(exc), due=time.time() + min(900, 30 * 2 ** (job['attempts'] - 1)))
            else:
                self.queue.finish(job['id'], 'failed', str(exc))
        except PickerError as exc:
            self.queue.finish(job['id'], 'uncertain' if writing else 'failed', str(exc))
        except Exception:
            self.queue.finish(job['id'], 'uncertain' if writing else 'failed', 'internal_error')
        return True

    def loop(self, stop):
        self.queue.recover()
        while not stop.is_set():
            self.run_once()
            stop.wait(1)


def decode(body, content_type):
    if len(body) > 16_384:
        raise PickerError('request_too_large')
    try:
        if content_type.split(';')[0] in ('application/json', 'text/plain'):
            data = json.loads(body)
        elif content_type.split(';')[0] == 'application/x-www-form-urlencoded':
            form = parse_qs(body.decode(), keep_blank_values=True, max_num_fields=20)
            if any(len(v) != 1 for v in form.values()):
                raise ValueError()
            data = {k: v[0] for k, v in form.items()}
        else:
            raise ValueError()
        if not isinstance(data, dict) or set(data) - {'deal_id', 'activity_id', 'secret'}:
            raise ValueError()
        return data
    except (ValueError, UnicodeError):
        raise PickerError('invalid_request') from None


def handler(config, queue, worker_thread):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'ProductPicker'

        def log_message(self, format, *args):
            pass  # Never log URLs, payloads, API errors, emails or secret-bearing headers.

        def setup(self):
            super().setup()
            self.connection.settimeout(10)

        def send_json(self, status, data):
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def authorized(self, secret):
            return isinstance(secret, str) and hmac.compare_digest(config.secret, secret)

        def do_GET(self):
            path = urlsplit(self.path)
            if path.query or path.fragment:
                return self.send_json(400, {'error': 'query_not_allowed'})
            if path.path == '/health':
                alive = worker_thread.is_alive()
                return self.send_json(200 if alive else 503, {'status': 'ok' if alive else 'worker_unavailable', 'write_enabled': config.write_enabled})
            if not self.authorized(self.headers.get('X-Webhook-Secret', '')):
                return self.send_json(401, {'error': 'unauthorized'})
            if path.path.startswith('/jobs/'):
                job = queue.get(path.path.removeprefix('/jobs/'))
                if job:
                    return self.send_json(200, {'id': job['id'], 'state': job['state'], 'error': job['error'],
                        'result': json.loads(job['result']) if job['result'] else None})
            self.send_json(404, {'error': 'not_found'})

        def do_POST(self):
            if self.path != '/bitrix/email-pick':
                return self.send_json(404, {'error': 'not_found'})
            try:
                if self.headers.get('Transfer-Encoding'):
                    raise PickerError('transfer_encoding_not_supported')
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 16_384:
                    return self.send_json(413, {'error': 'request_too_large'})
                body = self.rfile.read(length)
                if len(body) != length:
                    raise PickerError('incomplete_request')
                data = decode(body, self.headers.get('Content-Type', ''))
                secret = self.headers.get('X-Webhook-Secret') or data.get('secret')
                if not self.authorized(secret):
                    return self.send_json(401, {'error': 'unauthorized'})
                deal_id = identifier(data.get('deal_id'))
                activity_id = identifier(data['activity_id']) if data.get('activity_id') not in (None, '', 0, '0') else 0
                # Clients cannot turn write mode on; this is a server-side setting.
                job = queue.enqueue(deal_id, activity_id, config.write_enabled)
                self.send_json(202, job)
            except RetryLater:
                self.send_json(503, {'error': 'queue_full'})
            except (PickerError, ValueError, OSError):
                self.send_json(400, {'error': 'invalid_request'})
            except Exception:
                self.send_json(500, {'error': 'internal_error'})
    return Handler


def main():
    os.umask(0o077)
    config = Config.from_env()
    # Single-owner lock prevents two workers from recovering one another's in-flight writes.
    import fcntl
    Path(config.state_path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = open(config.state_path + '.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    queue = Queue(config.state_path)
    pipeline = Pipeline(Bitrix(config), Catalog(config), AI(config))
    stop = threading.Event()
    worker = Worker(config, queue, pipeline)
    thread = threading.Thread(target=worker.loop, args=(stop,), daemon=True)
    thread.start()
    server = ThreadingHTTPServer(('0.0.0.0', 8091), handler(config, queue, thread))
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()
        lock.close()


if __name__ == '__main__':
    main()
