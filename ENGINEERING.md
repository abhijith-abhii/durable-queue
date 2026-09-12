# Engineering and interview notes

## Why this project

Background jobs expose failure cases hidden by a basic CRUD application. This project adds complementary evidence in transactions, concurrency, state machines, and recovery. Its scope is intentionally single-host and observable from a terminal.

## Decisions

**SQLite instead of a broker.** One file makes the project easy to run and inspect. `BEGIN IMMEDIATE` serializes claimers through SQLite's writer lock, and the lease update commits before the handler starts. This trades write throughput and cross-host operation for a small, reproducible implementation. Each operation uses its own connection.

**At-least-once execution attempts.** A crashed worker leaves a lease that another worker can reclaim. Every claim consumes an attempt, including claims whose worker crashes. Delivery stops after a finite budget, so successful completion is not guaranteed.

**A new random token per lease.** Job ID alone would let an old worker acknowledge a job after a new worker reclaimed it. Acknowledgements, failures, and renewals check the current token and expiry in the same transaction as the state update. This protects queue state but cannot fence an external payment, email, or file write.

**Submission idempotency.** A key identifies a kind, canonical JSON payload, and retry budget. A repeated matching submission returns the same job; changing those properties is an error. A duplicate submission does not reschedule the existing job, even if the new delay differs.

**Bounded retries.** Failures return to ready with exponential delay or enter dead-letter state. Explicit replay resets the budget without changing the payload or key. There is no automatic jitter or fairness guarantee.

## Walkthrough

1. Run `python demo.py` and explain each output event.
2. Trace `enqueue → claim → execute → acknowledge` in `queuekit.py`.
3. Explain the crash window between handler side effects and acknowledgement.
4. Read the stale-worker and concurrent-connection tests. Explain the invariant each protects.
5. Change a handler, write a behavioral test, and demonstrate the result before presenting that change as your own contribution.

## Challenges worth discussing

- The ownership check and state mutation must share a transaction.
- An expired final attempt must enter dead-letter state rather than loop forever.
- A worker cannot renew a lease after it has expired, even if no one reclaimed it yet.
- Retrying deterministic invalid input is usually wasteful; a production handler would distinguish transient and permanent failures.

## Contribution record

The initial implementation, tests, examples, and documentation were generated and locally checked by Codex during this task. The user's later code review, design choices, modifications, and validation have not yet been established. Present it as a new AI-assisted personal portfolio project and describe only the parts you can explain and have reviewed. Do not describe this as employment, a historical university assignment, or a production system.

## Interview language

Accurate now: “This portfolio example uses SQLite transactions and expiring lease tokens to demonstrate background-job recovery. I am reviewing its state transitions and failure tests.”

After you have reviewed and run it yourself: explain the actual review or change you performed, then discuss a concrete test and tradeoff. Avoid claims about production scale, users, throughput improvements, or exactly-once side effects.
