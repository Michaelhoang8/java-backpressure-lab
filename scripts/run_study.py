"""Compile Java, run deterministic tests and workload trials, then render the report."""

from pathlib import Path
import argparse
import csv
import hashlib
import json
import subprocess

import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]


def rows(path):
    with path.open(newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--reuse", action="store_true", help="Replot existing Java measurements"
    )
    args = parser.parse_args()
    out = args.output_dir.resolve()
    sources = sorted((ROOT / "src/lab").glob("*.java"))
    java_hashes = {
        p.relative_to(ROOT)
        .as_posix(): hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n"))
        .hexdigest()
        for p in sources
    }
    if args.reuse:
        recorded_path = out / "results/source-sha256.json"
        if not recorded_path.exists():
            parser.error(
                "--reuse requires an existing source manifest from this driver"
            )
        recorded = json.loads(recorded_path.read_text(encoding="utf-8"))
        if any(recorded.get(name) != digest for name, digest in java_hashes.items()):
            parser.error("Java sources changed; run fresh measurements without --reuse")
    classes = out / "build/classes"
    raw = out / "results/raw"
    charts = out / "docs/charts"
    for directory in [classes, raw, charts]:
        directory.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["javac", "--release", "21", "-d", str(classes), *map(str, sources)],
        check=True,
        timeout=60,
    )
    tests = []
    for suite in ["lab.PipelineTests", "lab.PipelineReviewTests"]:
        result = subprocess.run(
            ["java", "-cp", str(classes), suite],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
        tests.append(result.stdout)
        print(result.stdout, end="")
    (out / "results/test-results.txt").write_text(
        "\n".join(tests), encoding="utf-8", newline="\n"
    )
    if not args.reuse:
        subprocess.run(
            [
                "java",
                "-cp",
                str(classes),
                "lab.PipelineStudy",
                str(raw),
                "20" if args.quick else "60",
            ],
            check=True,
            timeout=180,
        )
    provenance_sources = [*sources, ROOT / "scripts/run_study.py"]
    source_hashes = {
        p.relative_to(ROOT)
        .as_posix(): hashlib.sha256(p.read_bytes().replace(b"\r\n", b"\n"))
        .hexdigest()
        for p in provenance_sources
    }
    (out / "results/source-sha256.json").write_text(
        json.dumps(source_hashes, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    render(out)


def render(out):
    raw = out / "results/raw"
    charts = out / "docs/charts"
    summaries = [json.loads(p.read_text()) for p in sorted(raw.glob("*-summary.json"))]
    summaries.sort(key=lambda s: (s["policy"], s["capacity"], s["load"]))
    environment = json.loads((raw / "environment.json").read_text())
    all_tasks = {
        s["scenario"]: rows(raw / f'{s["scenario"]}-tasks.csv') for s in summaries
    }
    expanded = []
    for s in summaries:
        tasks = all_tasks[s["scenario"]]
        if (
            len(tasks) != s["attempted"]
            or s["attempted"] != s["admitted"] + s["rejected"]
        ):
            raise AssertionError("Admission counts do not reconcile")
        if s["admitted"] != s["completed"] + s["failed"] + s["cancelled"]:
            raise AssertionError("Final ownership counts do not reconcile")
        for status in ["completed", "failed", "cancelled", "rejected"]:
            if sum(r["outcome"] == status.upper() for r in tasks) != s[status]:
                raise AssertionError("Task outcomes disagree with summary")
        successful = [
            int(r["end_to_end_ns"]) / 1e6 for r in tasks if r["outcome"] == "COMPLETED"
        ]
        admission = [int(r["admission_wait_ns"]) / 1e6 for r in tasks]
        expanded.append(
            {
                **s,
                "completed_p50_ms": float(np.percentile(successful, 50)),
                "completed_p95_ms": float(np.percentile(successful, 95)),
                "admission_all_attempts_p95_ms": float(np.percentile(admission, 95)),
            }
        )
    with (out / "results/summary.csv").open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(expanded[0]))
        writer.writeheader()
        writer.writerows(expanded)
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 11,
            "figure.facecolor": "#faf8f2",
            "axes.facecolor": "#faf8f2",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.titlelocation": "left",
            "axes.titleweight": "bold",
            "savefig.dpi": 150,
        }
    )
    colors = {"COMPLETED": "#164a69", "FAILED": "#b75c42", "REJECTED": "#c4bca9"}
    labels = [s["scenario"] for s in expanded]
    positions = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(12, 6.6))
    left = np.zeros(len(labels))
    for status in colors:
        values = np.array([s[status.lower()] for s in expanded])
        ax.barh(
            positions, values, left=left, color=colors[status], label=status.title()
        )
        left += values
    ax.set_yticks(positions, labels)
    ax.invert_yaxis()
    ax.set_xlabel("Attempted tasks")
    fig.legend(frameon=False, ncol=3, loc="lower center")
    ax.set_title("Task outcomes by policy, capacity and load", fontsize=20, pad=18)
    fig.tight_layout(rect=(0, 0.055, 1, 1))
    fig.savefig(charts / "outcomes.png")
    plt.close(fig)
    fig, axes = plt.subplots(2, 1, figsize=(12, 7))
    for ax, scenario in zip(axes, ["block-c8-burst", "reject-c8-burst"]):
        samples = rows(raw / f"{scenario}-occupancy.csv")
        times = [int(r["time_ns"]) / 1e6 for r in samples]
        ax.step(
            times,
            [int(r["queued"]) for r in samples],
            where="post",
            label="Queued",
            color="#164a69",
        )
        ax.step(
            times,
            [int(r["waiting_producers"]) for r in samples],
            where="post",
            label="Waiting producers",
            color="#be873e",
        )
        ax.axhline(8, color="#a98b6c", ls="--", lw=1, label="Queue capacity")
        ax.set_ylabel("Items / threads")
        ax.set_xlabel("Scenario clock, milliseconds")
        ax.set_title(scenario, fontsize=15)
        ax.legend(frameon=False, ncol=3)
    fig.tight_layout()
    fig.savefig(charts / "occupancy.png")
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(12, 6.6))
    medians = [s["completed_p50_ms"] for s in expanded]
    tails = [s["completed_p95_ms"] for s in expanded]
    ax.hlines(positions, medians, tails, color="#b2bdc2", lw=3)
    ax.scatter(medians, positions, color="#164a69", label="P50")
    ax.scatter(tails, positions, color="#be873e", marker="D", label="P95")
    ax.set_yticks(positions, labels)
    ax.invert_yaxis()
    ax.set_xlabel("Milliseconds, successful tasks only")
    ax.legend(frameon=False)
    ax.set_title("Request-to-completion latency", fontsize=20, pad=18)
    fig.tight_layout()
    fig.savefig(charts / "latency.png")
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(12, 7))
    selected = all_tasks["block-c8-burst"][:40]
    for i, task in enumerate(selected):
        request = int(task["requested_ns"]) / 1e6
        admit = int(task["admitted_ns"]) / 1e6
        start = int(task["started_ns"]) / 1e6
        finish = int(task["finished_ns"]) / 1e6
        if admit >= 0:
            ax.broken_barh(
                [(request, admit - request)], (i - 0.35, 0.7), facecolors="#be873e"
            )
            ax.broken_barh(
                [(admit, start - admit)], (i - 0.35, 0.7), facecolors="#b2bdc2"
            )
            ax.broken_barh(
                [(start, finish - start)],
                (i - 0.35, 0.7),
                facecolors="#164a69" if task["outcome"] == "COMPLETED" else "#b75c42",
            )
        else:
            ax.plot([request, finish], [i, i], color="#b75c42", lw=1)
            ax.scatter([finish], [i], color="#b75c42", marker="x", s=16)
    ax.set_yticks(
        range(0, len(selected), 4),
        [selected[i]["task_id"] for i in range(0, len(selected), 4)],
    )
    ax.invert_yaxis()
    ax.set_ylabel("Task ID (first 40 recorded attempts)")
    ax.set_xlabel("Scenario clock, milliseconds")
    ax.set_title(
        "Admission, queue residence and service intervals", fontsize=20, pad=18
    )
    fig.text(
        0.02,
        0.015,
        "Gold: admission wait  |  Gray: queued  |  Navy: completed service  |  Rust: failed/rejected",
        fontsize=10,
    )
    fig.tight_layout(rect=(0, 0.035, 1, 1))
    fig.savefig(charts / "timeline.png")
    plt.close(fig)
    findings(out, expanded, environment)


def findings(out, data, env):
    attempts = sum(s["attempted"] for s in data)
    table = "| Scenario | Admitted | Rejected | Completed/s | Completed P50 ms | Completed P95 ms |\n|---|---:|---:|---:|---:|---:|\n"
    for s in data:
        table += f'| {s["scenario"]} | {s["admitted"]} | {s["rejected"]} | {s["completed_per_second"]:.1f} | {s["completed_p50_ms"]:.1f} | {s["completed_p95_ms"]:.1f} |\n'
    text = f"""# Local workload results

{len(data)} actual Java-thread scenarios and {attempts:,} attempted tasks. Recorded {env['recorded_at_utc']}; JDK {env['java_version']}, {env['java_vendor']}, {env['os']} {env['architecture']}.

{table}

Successful completion rate counts COMPLETED only, divided by elapsed time from simultaneous producer release through graceful worker termination. Failed tasks consumed service time but are not counted as successful completions. All attempted task records remain in the CSV, including rejections and failures.

P50/P95 latency includes only successful tasks. A rejecting policy can show short latency precisely because it admits less work; compare rejection counts alongside latency. This is not an equal-completed-work benchmark. Occupancy is sampled, so exact peak queue occupancy comes from lock-protected enqueue bookkeeping, not sampling.

The workload requests a minimum two-millisecond parked service delay plus integer arithmetic. Java scheduler/timer behavior can overshoot that request substantially; observed service and producer intervals are in the trace. The paced case requests a four-millisecond producer pause, not a guaranteed arrival rate. Each scenario runs once, in fixed order, without JMH warmup/isolation. These results demonstrate overload tradeoffs on this machine, not production throughput or statistical significance.
"""
    (out / "results/findings.md").write_text(text, encoding="utf-8", newline="\n")
    table_html = "".join(
        f'<tr><td>{s["scenario"]}</td><td>{s["admitted"]}</td><td>{s["rejected"]}</td><td>{s["completed_per_second"]:.1f}</td><td>{s["completed_p95_ms"]:.1f}</td></tr>'
        for s in data
    )
    page = f"""<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Java backpressure study</title><style>*{{box-sizing:border-box}}body{{margin:0;background:#faf8f2;color:#1a3548;font:16px/1.6 system-ui,sans-serif}}header{{padding:44px 6vw 35px;border-bottom:1px solid #d4d2c8}}h1{{font-size:54px;line-height:1.1;letter-spacing:-2px;margin:15px 0}}h2{{font-size:30px;letter-spacing:-.5px;margin:12px 0 22px}}h3{{font-size:17px}}.tag{{font:12px ui-monospace,monospace;letter-spacing:2px;color:#85682f}}main{{max-width:1320px;margin:auto;padding:0 45px}}section{{padding:42px 0;border-bottom:1px solid #d4d2c8;scroll-margin-top:10px}}nav{{display:flex;gap:25px;font:13px ui-monospace,monospace;margin-top:24px}}a{{color:#1a5877;text-decoration:none}}.grid{{display:grid;grid-template-columns:1fr 1fr;gap:24px}}.panel{{background:white;border:1px solid #d4d2c8;padding:22px}}.stats{{display:flex;gap:42px;margin-top:28px}}.stats b{{font-size:37px;display:block}}.stats span,.muted{{font-size:13px;color:#626e73}}img{{width:100%;display:block}}code,pre{{font-family:ui-monospace,Consolas,monospace;font-size:13px}}pre{{white-space:pre-wrap}}.formula{{background:#1a3548;color:#fff;padding:24px;margin:22px 0}}table{{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;font-size:14px}}td,th{{padding:13px 9px;text-align:right;border-bottom:1px solid #d4d2c8}}td:first-child,th:first-child{{text-align:left}}th{{font:11px ui-monospace,monospace;text-transform:uppercase}}footer{{padding:32px 0;font-size:12px;color:#626e73}}@media(max-width:800px){{main{{padding:0 20px}}.grid{{grid-template-columns:1fr}}h1{{font-size:37px}}nav{{flex-wrap:wrap}}.stats{{gap:20px}}}}</style>
<header id="overview"><div class="tag">JAVA 21 / CONCURRENCY / LOCAL WORKLOAD STUDY</div><h1>Bounded pipeline<br>backpressure study</h1><p>Compare timed blocking admission and immediate rejection using actual producer and worker threads, explicit ownership accounting and graceful shutdown.</p><div class="stats"><div><b>{len(data)}</b><span>measured scenarios</span></div><div><b>{attempts:,}</b><span>attempted tasks</span></div><div><b>2</b><span>workers per scenario</span></div><div><b>3</b><span>queue capacities</span></div></div><nav><a href="#ownership">Ownership</a><a href="#occupancy">Occupancy</a><a href="#latency">Latency</a><a href="#accounting">Outcomes</a><a href="#method">Method</a></nav></header><main>
<section id="ownership"><div class="tag">01 / TASK LIFECYCLE</div><h2>Admission and shutdown semantics</h2><div class="grid"><div class="panel"><h3>BLOCK</h3><p>Wait for queue capacity using a condition and a bounded wait budget. Reject on timeout, interruption or closure. Restore an interrupted caller's flag.</p><h3>REJECT</h3><p>Return immediately when the waiting queue is full. No caller-runs fallback and no hidden unbounded queue.</p></div><div class="panel"><h3>OPEN → CLOSING → TERMINATED</h3><p>Closure rejects new ownership and wakes blocked submitters. Workers drain admitted work. Running tasks are not forcibly interrupted.</p><p>Only queued tasks can be cancelled. A task failure is recorded before the worker continues.</p></div></div><div class="formula"><code>attempted = admitted + rejected + waiting submitters<br>admitted = queued + running + completed + failed + cancelled</code></div><img src="charts/timeline.png" alt="Actual admission queue and service intervals"></section>
<section id="occupancy"><div class="tag">02 / QUEUE STATE</div><h2>Sampled occupancy and waiting producers</h2><img src="charts/occupancy.png" alt="Queue and blocked-producer occupancy for two policies"><p class="muted">Capacity 8, burst load. Each panel uses its own actual scenario duration. Samples can miss short-lived peaks; exact peak capacity is also tracked under the queue lock.</p></section>
<section id="latency"><div class="tag">03 / COMPLETION LATENCY</div><h2>Successful-task latency distributions</h2><img src="charts/latency.png" alt="P50 and P95 successful-task end-to-end latency"><div class="panel"><p>Lower latency under rejection does not mean more work was served. These distributions condition on successful completion; rejected and failed attempts remain visible in the outcome counts and raw records.</p></div></section>
<section id="accounting"><div class="tag">04 / OUTCOMES</div><h2>Every attempt has an outcome</h2><img src="charts/outcomes.png" alt="Completed failed and rejected task counts"><table><thead><tr><th>Scenario</th><th>Admitted</th><th>Rejected</th><th>Completed/s</th><th>Completed P95 ms</th></tr></thead><tbody>{table_html}</tbody></table><p class="muted">Benchmarks inject a failure for admitted task IDs divisible by 17. Cancellation is exercised by deterministic tests, not injected into these measurement scenarios.</p></section>
<section id="method"><div class="tag">05 / IMPLEMENTATION AND REPRODUCTION</div><h2>Java executes the concurrent workload.</h2><div class="grid"><div class="panel"><h3>Implementation</h3><p>ReentrantLock, conditions, a bounded ArrayDeque and owned worker threads. Latches establish queue saturation in tests. Python compiles/runs Java and plots recorded results.</p><pre>python scripts/run_study.py
python scripts/run_study.py --quick \
  --output-dir build/my-run</pre><p class="muted">Raw task timestamps, occupancy observations, JDK metadata and source hashes are committed.</p></div><div class="panel"><h3>Measurement limits</h3><p>Local finite synthetic workloads. Requested 2 ms service delay and 4 ms paced-producer pause can overshoot with OS scheduling. Fixed scenario order; no JMH-grade warmup or isolation.</p><p>Queue capacity bounds waiting work, not lifetime telemetry retention. Fatal VM failures are outside the recovery guarantee.</p></div></div><p class="muted">JDK {env['java_version']} · {env['java_vendor']} · {env['os']} {env['architecture']} · recorded {env['recorded_at_utc']}</p></section><footer>AI-assisted portfolio study. Actual Java-thread measurements; synthetic work. No network activity and no claim of production throughput or exactly-once external effects.</footer></main></html>"""
    page = page.replace("immediate rejection", "rejection without a queue-space wait")
    page = page.replace(
        "Return immediately when the waiting queue is full.",
        "Reject a full waiting queue after acquiring the state lock; never wait for queue capacity.",
    )
    page = page.replace(
        "Java executes the concurrent workload.", "Implementation and measurements"
    )
    page = page.replace("Every attempt has an outcome", "Task outcomes and accounting")
    page = page.replace(
        "Fixed scenario order; no JMH-grade warmup or isolation.",
        "One trial per scenario in fixed order; JIT warmup and order are confounders. No JMH-grade isolation.",
    )
    (out / "docs/report.html").write_text(page, encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
