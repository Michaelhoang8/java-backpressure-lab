# Local workload results

12 actual Java-thread scenarios and 2,160 attempted tasks. Recorded 2026-11-20T23:09:30.120445500Z; JDK 21.0.12.1, Eclipse Adoptium, Windows 11 amd64.

| Scenario | Admitted | Rejected | Completed/s | Completed P50 ms | Completed P95 ms |
|---|---:|---:|---:|---:|---:|
| block-c2-burst | 94 | 86 | 118.7 | 32.1 | 47.9 |
| block-c2-paced | 155 | 25 | 120.3 | 31.9 | 47.9 |
| block-c8-burst | 102 | 78 | 118.4 | 93.0 | 95.5 |
| block-c8-paced | 154 | 26 | 118.0 | 78.6 | 92.2 |
| block-c32-burst | 112 | 68 | 123.7 | 236.0 | 285.8 |
| block-c32-paced | 167 | 13 | 134.0 | 161.5 | 269.1 |
| reject-c2-burst | 4 | 176 | 105.0 | 28.5 | 28.5 |
| reject-c2-paced | 121 | 59 | 122.0 | 31.0 | 32.0 |
| reject-c8-burst | 10 | 170 | 119.4 | 44.8 | 75.3 |
| reject-c8-paced | 127 | 53 | 117.4 | 77.5 | 79.1 |
| reject-c32-burst | 34 | 146 | 117.1 | 139.1 | 240.6 |
| reject-c32-paced | 152 | 28 | 121.4 | 221.2 | 269.6 |


Successful completion rate counts COMPLETED only, divided by elapsed time from simultaneous producer release through graceful worker termination. Failed tasks consumed service time but are not counted as successful completions. All attempted task records remain in the CSV, including rejections and failures.

P50/P95 latency includes only successful tasks. A rejecting policy can show short latency precisely because it admits less work; compare rejection counts alongside latency. This is not an equal-completed-work benchmark. Occupancy is sampled, so exact peak queue occupancy comes from lock-protected enqueue bookkeeping, not sampling.

The workload requests a minimum two-millisecond parked service delay plus integer arithmetic. Java scheduler/timer behavior can overshoot that request substantially; observed service and producer intervals are in the trace. The paced case requests a four-millisecond producer pause, not a guaranteed arrival rate. Each scenario runs once, in fixed order, without JMH warmup/isolation. These results demonstrate overload tradeoffs on this machine, not production throughput or statistical significance.
