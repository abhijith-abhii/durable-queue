"""Run an isolated, deterministic queue recovery demonstration."""
import json
from pathlib import Path
import tempfile
from queuekit import LeaseLost, Queue, work_once


def main():
    with tempfile.TemporaryDirectory() as folder:
        now = [100.0]
        queue = Queue(Path(folder) / 'demo.db', clock=lambda: now[0])
        first = queue.enqueue('welcome', 'uppercase', {'text': 'hello recruiter'})
        assert queue.enqueue('welcome', 'uppercase', {'text': 'hello recruiter'}) == first
        work_once(queue)
        print(json.dumps({'event': 'completed', 'result': queue.inspect(first)['result']}))
        queue.enqueue('recovery', 'sum', {'numbers': [10, 20, 30]})
        stale = queue.claim(5)
        now[0] += 5
        recovered = queue.claim(5)
        try:
            queue.acknowledge(stale['id'], stale['token'], {'total': -1})
        except LeaseLost:
            print(json.dumps({'event': 'stale_worker_rejected'}))
        else:
            raise AssertionError('stale worker was accepted')
        queue.acknowledge(recovered['id'], recovered['token'], {'total': 60})
        queue.enqueue('invalid', 'sum', {'numbers': ['invalid']}, max_attempts=1)
        work_once(queue)
        assert queue.stats() == {'ready': 0, 'running': 0, 'done': 2, 'dead': 1}
        print(json.dumps({'event': 'final_counts', **queue.stats()}))


if __name__ == '__main__':
    main()
