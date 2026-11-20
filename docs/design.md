# Queue ownership and lifecycle

## State under one lock

The waiting `ArrayDeque`, retained task map, lifecycle state and all counters share a `ReentrantLock`. The queue is FIFO by successful enqueue order, which is not necessarily request timestamp or task ID order. The lock is nonfair: producer admission fairness is not promised.

There are three conditions: `available` wakes workers, `space` wakes blocked submitters, and `changed` supports lifecycle and diagnostic waits. Predicates are always rechecked in loops. Both cancellation and worker dequeue release capacity. Closure wakes all worker and submitter waiters.

The constructor starts owned worker threads. The supported lifecycle is `OPEN → CLOSING → TERMINATED`; there is no reopen. Thread creation and fatal VM failures are outside the recovery contract. `close()` is idempotent and returns without waiting for work. `awaitTermination()` observes worker termination, not caller-owned producer threads.

## Admission contract

Each unique, valid call enters the task map while holding the lock. A repeated ID throws `IllegalArgumentException`; it never overwrites the original record or adds to `attempted`. Invalid arguments and durations overflowing nanoseconds also do not create task ownership.

`REJECT` never waits on the queue-space condition. It may wait to acquire the shared lock, so it is not wait-free or a hard latency guarantee. A full queue yields reason `FULL`.

`BLOCK` waits with the remaining `Condition.awaitNanos` budget while the pipeline is open and the queue is full. Reason `TIMEOUT` means the budget is exhausted and the queue is still full when rechecked. Space available at recheck permits admission, even after scheduling overshoot. The budget excludes initial lock acquisition and is not an end-to-end deadline. A zero budget still admits if space is available.

An interrupted call yields reason `INTERRUPTED` and restores the caller's interrupt flag. A closed pipeline yields `CLOSED`. A pre-interrupted submitter is rejected before the closure check, so `INTERRUPTED` takes precedence. `submit` returns whether ownership was admitted, not whether execution succeeded.

## Task transitions

```text
WAITING ── capacity available ──> QUEUED ── worker claims ──> RUNNING
   │                               │                          │
   └─ full/timeout/close/interrupt  └─ cancelQueued             ├─ return → COMPLETED
             ↓                          ↓                     └─ throw  → FAILED
          REJECTED                    CANCELLED
```

Workers remove tasks and mark them `RUNNING` atomically under the lock, then invoke user work outside it. Cancellation only succeeds while the task is `QUEUED`; the dequeue/cancel race has one winner. Task payload references are cleared when claimed, cancelled or rejected; scalar telemetry remains available for the life of the pipeline.

A worker records a task failure and continues. The code catches `Throwable` to account for demonstrable task failures such as `AssertionError`; it does not guarantee a usable VM after `OutOfMemoryError`, other fatal errors or thread-creation failure. An interrupt left by one task is cleared before invoking the next task, isolating successive work items. Shutdown itself uses state and conditions, not worker interrupts.

## Accounting invariants

At every externally observable lock-consistent snapshot:

```text
attempted = admitted + rejected + waitingProducers
admitted  = queued + running + completed + failed + cancelled
0 <= queued <= capacity
```

A submitter waiting for the state lock is not yet included in `attempted`. A submitter waiting on `space` is included in `waitingProducers`. State changes within a lock critical section cannot be observed midway. Final results contain one terminal record for each unique valid attempt once producers have returned and workers have terminated.

## What is bounded

Only the waiting queue's item count is bounded. Up to `workers` additional items can run. Caller threads can wait for admission and retain their closures. The diagnostic task map grows with total attempts. This deliberate observability tradeoff supports finite experiments; a production implementation would need history eviction, metrics aggregation and an explicit memory budget.
