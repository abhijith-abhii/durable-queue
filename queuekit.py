"""A local durable job queue with leases and fenced acknowledgements."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
from pathlib import Path
import sqlite3
import time
import uuid


class Conflict(ValueError):
    """An idempotency key was reused for different work."""


class LeaseLost(RuntimeError):
    """The worker no longer owns an unexpired job lease."""


class Queue:
    def __init__(self, database, clock=time.time):
        self.database = str(database)
        self.clock = clock
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE,
                    kind TEXT NOT NULL, payload TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'ready'
                        CHECK(state IN ('ready','running','done','dead')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    available_at REAL NOT NULL, created_at REAL NOT NULL,
                    lease_until REAL, token TEXT, result TEXT, last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS jobs_ready
                    ON jobs(state, available_at, id);
            """)

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.database, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def transaction(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def positive(value, label):
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{label} must be a finite positive number")

    def enqueue(self, key, kind, payload, max_attempts=3, delay=0):
        if not isinstance(key, str) or not key.strip() or not kind:
            raise ValueError("nonempty key and kind are required")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer from 1 to 100")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay must be finite and nonnegative")
        encoded = json.dumps(payload, sort_keys=True, allow_nan=False)
        now = self.clock()
        with self.transaction() as db:
            old = db.execute("SELECT * FROM jobs WHERE key=?", (key,)).fetchone()
            if old:
                if (old['kind'], old['payload'], old['max_attempts']) != (
                        kind, encoded, max_attempts):
                    raise Conflict("key already identifies different work")
                return old['id']
            return db.execute("""INSERT INTO jobs
                (key,kind,payload,max_attempts,available_at,created_at)
                VALUES (?,?,?,?,?,?)""", (
                    key, kind, encoded, max_attempts, now + delay, now
                )).lastrowid

    def claim(self, lease_seconds=30):
        self.positive(lease_seconds, 'lease_seconds')
        with self.transaction() as db:
            now = self.clock()
            # Reclaim expired jobs; a crashed last attempt goes to dead-letter.
            db.execute("""UPDATE jobs SET state=CASE
                WHEN attempts >= max_attempts THEN 'dead' ELSE 'ready' END,
                token=NULL, lease_until=NULL, available_at=?,
                last_error='lease expired before acknowledgement'
                WHERE state='running' AND lease_until <= ?""", (now, now))
            row = db.execute("""SELECT * FROM jobs WHERE state='ready'
                AND available_at <= ? ORDER BY available_at,id LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            token = uuid.uuid4().hex
            db.execute("""UPDATE jobs SET state='running', attempts=attempts+1,
                token=?,lease_until=? WHERE id=?""", (token, now + lease_seconds, row['id']))
            job = dict(db.execute("SELECT * FROM jobs WHERE id=?", (row['id'],)).fetchone())
            job['payload'] = json.loads(job['payload'])
            return job

    def _owned(self, db, job_id, token):
        row = db.execute("""SELECT * FROM jobs WHERE id=? AND token=?
            AND state='running' AND lease_until > ?""", (job_id, token, self.clock())).fetchone()
        if row is None:
            raise LeaseLost("lease expired or ownership changed")
        return row

    def acknowledge(self, job_id, token, result):
        encoded = json.dumps(result, allow_nan=False)
        with self.transaction() as db:
            self._owned(db, job_id, token)
            db.execute("""UPDATE jobs SET state='done',result=?,token=NULL,
                lease_until=NULL WHERE id=?""", (encoded, job_id))

    def heartbeat(self, job_id, token, lease_seconds=30):
        self.positive(lease_seconds, 'lease_seconds')
        with self.transaction() as db:
            self._owned(db, job_id, token)
            db.execute("UPDATE jobs SET lease_until=? WHERE id=?",
                       (self.clock() + lease_seconds, job_id))

    def fail(self, job_id, token, error, retry_base=1):
        self.positive(retry_base, 'retry_base')
        with self.transaction() as db:
            row = self._owned(db, job_id, token)
            state = 'dead' if row['attempts'] >= row['max_attempts'] else 'ready'
            delay = min(3600, retry_base * 2 ** (row['attempts'] - 1))
            db.execute("""UPDATE jobs SET state=?,available_at=?,last_error=?,
                token=NULL,lease_until=NULL WHERE id=?""",
                       (state, self.clock() + delay, str(error)[:1000], job_id))

    def retry_dead(self, job_id):
        with self.transaction() as db:
            updated = db.execute("""UPDATE jobs SET state='ready',attempts=0,
                available_at=?,last_error=NULL WHERE id=? AND state='dead'""",
                                 (self.clock(), job_id)).rowcount
            if updated != 1:
                raise ValueError("job is not in dead-letter state")

    def inspect(self, job_id):
        with self.connection() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise ValueError("job does not exist")
        result = dict(row)
        result.pop('token')  # A lease token should stay with its worker.
        result['payload'] = json.loads(result['payload'])
        result['result'] = json.loads(result['result']) if result['result'] else None
        return result

    def stats(self):
        counts = dict.fromkeys(('ready', 'running', 'done', 'dead'), 0)
        with self.connection() as db:
            counts.update(dict(db.execute("SELECT state,count(*) FROM jobs GROUP BY state")))
        return counts


def execute(job):
    """Bounded demo handlers; never execute arbitrary shell commands."""
    if job['kind'] == 'uppercase':
        text = job['payload']['text']
        if not isinstance(text, str):
            raise ValueError('text must be a string')
        return {'text': text.upper()}
    if job['kind'] == 'sum':
        numbers = job['payload']['numbers']
        if not isinstance(numbers, list) or any(
            type(n) not in (int, float) or not math.isfinite(n) for n in numbers
        ):
            raise ValueError('numbers must be a list of finite numbers')
        return {'total': sum(numbers)}
    raise ValueError(f"unknown job kind: {job['kind']}")


def work_once(queue, lease_seconds=30):
    job = queue.claim(lease_seconds)
    if job is None:
        return False
    try:
        result = execute(job)
    except Exception as error:
        queue.fail(job['id'], job['token'], error)
    else:
        queue.acknowledge(job['id'], job['token'], result)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='var/queue.db')
    commands = parser.add_subparsers(dest='command', required=True)
    enqueue = commands.add_parser('enqueue')
    enqueue.add_argument('--key', required=True)
    enqueue.add_argument('--kind', choices=['uppercase', 'sum'], required=True)
    enqueue.add_argument('--payload', required=True, help='JSON payload')
    enqueue.add_argument('--max-attempts', type=int, default=3)
    worker = commands.add_parser('worker')
    worker.add_argument('--once', action='store_true')
    worker.add_argument('--lease', type=float, default=30)
    commands.add_parser('stats')
    for name in ('inspect', 'retry'):
        commands.add_parser(name).add_argument('id', type=int)
    args = parser.parse_args()
    queue = Queue(args.db)
    try:
        if args.command == 'enqueue':
            print(json.dumps({'id': queue.enqueue(args.key, args.kind,
                json.loads(args.payload), args.max_attempts)}))
        elif args.command == 'worker':
            while True:
                handled = work_once(queue, args.lease)
                if args.once:
                    print(json.dumps({'handled': handled}))
                    break
                if not handled:
                    time.sleep(0.25)
        elif args.command == 'stats':
            print(json.dumps(queue.stats()))
        elif args.command == 'inspect':
            print(json.dumps(queue.inspect(args.id), indent=2))
        elif args.command == 'retry':
            queue.retry_dead(args.id)
            print(json.dumps({'retried': args.id}))
    except (ValueError, LeaseLost, sqlite3.Error) as error:
        parser.exit(2, f'Error: {error}\n')
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
