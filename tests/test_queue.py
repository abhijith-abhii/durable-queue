from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from queuekit import Conflict, LeaseLost, Queue, work_once


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100.0
        self.path = Path(self.tmp.name) / 'jobs.db'
        self.queue = Queue(self.path, clock=lambda: self.now)

    def add(self, key='one', **kwargs):
        return self.queue.enqueue(key, 'uppercase', {'text': 'hello'}, **kwargs)

    def test_persists_and_processes(self):
        job_id = self.add()
        other = Queue(self.path, clock=lambda: self.now)
        self.assertTrue(work_once(other))
        self.assertEqual(self.queue.inspect(job_id)['result'], {'text': 'HELLO'})
        self.assertEqual(self.queue.stats()['done'], 1)
        self.assertFalse(work_once(other))

    def test_idempotency_and_conflict(self):
        self.assertEqual(self.add(), self.add())
        with self.assertRaises(Conflict):
            self.queue.enqueue('one', 'sum', {'numbers': [1]})
        self.assertEqual(self.queue.stats()['ready'], 1)

    def test_stale_worker_cannot_acknowledge_or_fail(self):
        job_id = self.add()
        old = self.queue.claim(5)
        self.now += 5
        new = self.queue.claim(5)
        with self.assertRaises(LeaseLost):
            self.queue.acknowledge(job_id, old['token'], 'wrong')
        with self.assertRaises(LeaseLost):
            self.queue.fail(job_id, old['token'], 'wrong')
        self.queue.acknowledge(job_id, new['token'], 'correct')
        self.assertEqual(self.queue.inspect(job_id)['result'], 'correct')

    def test_expired_last_attempt_becomes_dead(self):
        job_id = self.add(max_attempts=1)
        self.queue.claim(5)
        self.now += 5
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.inspect(job_id)['state'], 'dead')
        self.queue.retry_dead(job_id)
        self.assertIsNotNone(self.queue.claim())

    def test_exponential_backoff_and_dead_letter(self):
        job_id = self.add(max_attempts=2)
        job = self.queue.claim()
        self.queue.fail(job_id, job['token'], 'temporary')
        self.assertIsNone(self.queue.claim())
        self.now += 1
        job = self.queue.claim()
        self.queue.fail(job_id, job['token'], 'again')
        self.assertEqual(self.queue.inspect(job_id)['state'], 'dead')
        self.now += 100
        self.assertIsNone(self.queue.claim())

    def test_heartbeat_and_delayed_jobs(self):
        self.add(delay=2)
        self.assertIsNone(self.queue.claim())
        self.now += 2
        job = self.queue.claim(2)
        self.now += 1
        self.queue.heartbeat(job['id'], job['token'], 5)
        self.now += 2
        self.assertIsNone(self.queue.claim())
        self.queue.acknowledge(job['id'], job['token'], None)

    def test_concurrent_connections_claim_each_job_once(self):
        for i in range(40):
            self.add(str(i))
        def consume(_):
            job = self.queue.claim(100)
            if job:
                self.queue.acknowledge(job['id'], job['token'], 'done')
                return job['id']
        with ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(consume, range(48)))
        ids = [i for i in ids if i is not None]
        self.assertEqual(len(ids), 40)
        self.assertEqual(len(set(ids)), 40)

    def test_invalid_payload_and_nonfinite_options(self):
        with self.assertRaises(ValueError):
            self.queue.enqueue('x', 'sum', {'numbers': [float('nan')]})
        for invalid in (0, -1, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                self.queue.claim(invalid)
        self.queue.enqueue('bad', 'sum', {'numbers': ['oops']}, max_attempts=1)
        work_once(self.queue)
        self.assertEqual(self.queue.stats()['dead'], 1)


if __name__ == '__main__':
    unittest.main()
