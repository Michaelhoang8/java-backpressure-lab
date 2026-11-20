# Correctness evidence

The test suites use `CountDownLatch` and a state-based condition wait to establish queue saturation. Timeouts bound a failed test; sleeping for an assumed scheduling order is not the assertion mechanism. Every suite runs on real Java threads.

| Test | Established behavior |
|---|---|
| Saturated REJECT queue | One running task and one waiting task fill capacity; the next attempt is rejected and never executes. |
| BLOCK admission resumes | A third submitter demonstrably waits for capacity, then is admitted when a worker claims queued work. |
| Closure during saturation | Close wakes a waiting producer with `CLOSED`, rejects subsequent submissions and drains admitted tasks. |
| Interruption | A blocked submitter returns a rejection with its interrupt flag restored; interrupted termination waits also restore the flag. |
| Timeout and cancellation | Zero-budget saturation rejects; queued cancellation frees capacity; running/double cancellation cannot steal ownership. |
| Worker failure isolation | Runtime exception and assertion failure are accounted for; later work executes; a task's leftover interrupt does not leak into the next task. |
| Close/submit race | Forty barrier-started rounds race 30 submissions with closure; submissions known to begin after close returns cannot be admitted. |
| Concurrent duplicate IDs | Eight producers race the same ID: one owner and execution, seven validation errors, no overwritten history. |
| Positive timeout and cancellation wakeup | A nonzero wait times out with no ownership; cancelling queued work wakes a blocked producer before the running task is released. |

The independent review suite additionally checks lifecycle timestamps are monotone and final counters reconcile. The raw-evidence Python tests independently recompute outcome totals and successful completion rates, verify lifecycle intervals and sampled bounds, and compare normalized source hashes with the committed source.

Passing these finite tests is evidence for the exercised interleavings, not a formal proof of all Java memory-model behaviors or scheduler fairness. There is no linearizability checker, stress qualification on many hardware architectures or claim of recovery from fatal JVM failures. CI reruns both Java suites on Ubuntu with JDK 21.

The study runner captures producer/sampler failures, bounds readiness and joins, and uses `finally` to close the pipeline, release any startup wait, interrupt producer cleanup, stop sampling and await finite service drain. The Python driver also bounds subprocess execution so reproduction failures do not hang indefinitely.
