# Measurement protocol

## Matrix and workload

The default run has 12 configurations: `BLOCK`/`REJECT`, capacities 2/8/32, and burst/paced arrivals. Each has two worker threads and three producer threads, each attempting 60 tasks. The quick CI run uses 20 tasks per producer. Task IDs are assigned atomically; actual interleavings and outcomes are deliberately measured rather than synthesized.

Service consists of 12,000 xorshift arithmetic iterations, a volatile sink write, and a requested minimum two-millisecond `LockSupport.parkNanos` delay. Admitted IDs divisible by 17 throw a synthetic exception after service. Paced producers request a minimum four-millisecond pause after each submission. Burst producers have no intentional pause. `BLOCK` gets a three-millisecond condition-wait budget. This models finite blocking service without performing external I/O.

All configurations run sequentially in a fixed order within one JVM: policy, capacity, then burst/paced. There is one trial per configuration and no untimed warmup. JIT compilation, order, background activity and OS timer granularity confound comparisons. These measurements illustrate queue behavior and admission tradeoffs, not statistically established policy rankings, production throughput or JMH-grade microbenchmarks.

## Clocks and rates

The pipeline has one `System.nanoTime()` origin. All exported timestamps are offsets from it, independent of wall-clock corrections. A scenario's rate interval begins just before releasing the producer start latch and ends after graceful worker termination. Thread creation and final output serialization are excluded. Worker drain and failed-task service are included.

`completed_per_second = completed / (finish_ns - start_ns) × 1e9` counts only successful completions. `FAILED` tasks consumed service but are not successes. `REJECTED` tasks were never owned by the queue. Benchmarks inject no cancellations; cancellation has deterministic test coverage.

| Field | Definition |
|---|---|
| `requested_ns` | Timestamp before acquiring the submission lock. |
| `admitted_ns` | Ownership acquired by successful enqueue; `-1` if rejected. |
| `started_ns` | Worker claim under the lock; `-1` if never started. |
| `finished_ns` | Completion, failure, cancellation or rejection recorded under lock. |
| `admission_wait_ns` | Admission minus request, or rejection minus request. Includes initial lock contention and scheduling. |
| `queue_wait_ns` | Worker claim minus admission; `-1` if never started. |
| `end_to_end_ns` | Finish minus request, for every terminal attempt. |

Summary P50/P95 uses NumPy's default linear percentile over successful tasks only. Admission P95 includes all attempts. Conditioning on success matters: rejecting most work can lower the displayed completion latency. The matrix does not hold completed work constant, so lower latency is not proof of a better policy.

## Occupancy and overhead

A separate thread samples queue depth, running tasks, waiting submitters and state, requesting a 0.5 ms park between observations. Its timestamps are recorded just before obtaining a snapshot; a sample is not a precise enqueue/dequeue event. Sampling can miss short intervals. Exact `peak_queued` is tracked during enqueue while holding the state lock. Sampling, locking, timestamping and retained task telemetry all add overhead to this instrumented implementation.

`parkNanos` is not a real-time timer. Windows timer/scheduler behavior in the committed run may make observed intervals much larger than the requested delays. The raw traces preserve those observations. Neither nominal delay nor nominal pacing is used to manufacture a result.

## Provenance and reproduction

`results/raw/environment.json` records actual JDK/runtime/vendor, OS/architecture, available logical processors, UTC capture time and workload parameters. `results/source-sha256.json` hashes the Java sources and report driver after normalizing CRLF to LF; this is portable across Git line-ending checkouts. The `.gitattributes` file requests LF text. Test transcripts are saved alongside raw measurements.

Reproduction produces fresh measurements rather than byte-identical timing records. Use `--output-dir build/my-run` to retain the committed evidence. Chart generation is deterministic for the same raw files and plotting versions, subject to platform font/rendering differences. `--reuse` must not be used to attach changed measurement-source hashes to an old trial.
