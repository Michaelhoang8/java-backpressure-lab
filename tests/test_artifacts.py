"""Independent consistency checks over committed Java measurement evidence."""

import csv
import hashlib
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "results/raw"


def read_rows(path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


class RecordedEvidenceTests(unittest.TestCase):
    def test_source_provenance(self):
        manifest = json.loads((ROOT / "results/source-sha256.json").read_text())
        self.assertIn("src/lab/BoundedPipeline.java", manifest)
        self.assertIn("src/lab/PipelineStudy.java", manifest)
        self.assertIn("scripts/run_study.py", manifest)
        for name, expected in manifest.items():
            with self.subTest(source=name):
                data = (ROOT / name).read_bytes().replace(b"\r\n", b"\n")
                self.assertEqual(hashlib.sha256(data).hexdigest(), expected)

    def test_matrix_and_terminal_accounting(self):
        summaries = list(RAW.glob("*-summary.json"))
        self.assertEqual(len(summaries), 12)
        seen = set()
        for path in summaries:
            summary = json.loads(path.read_text())
            scenario = summary["scenario"]
            with self.subTest(scenario=scenario):
                seen.add((summary["policy"], summary["capacity"], summary["load"]))
                tasks = read_rows(RAW / f"{scenario}-tasks.csv")
                self.assertEqual(len(tasks), summary["attempted"])
                self.assertEqual(len({r["task_id"] for r in tasks}), len(tasks))
                self.assertTrue(all(r["scenario"] == scenario for r in tasks))
                for outcome in ["completed", "failed", "cancelled", "rejected"]:
                    count = sum(r["outcome"] == outcome.upper() for r in tasks)
                    self.assertEqual(count, summary[outcome])
                self.assertEqual(
                    summary["attempted"], summary["admitted"] + summary["rejected"]
                )
                self.assertEqual(
                    summary["admitted"],
                    summary["completed"] + summary["failed"] + summary["cancelled"],
                )
                self.assertEqual(
                    summary["duration_ns"], summary["finish_ns"] - summary["start_ns"]
                )
                self.assertAlmostEqual(
                    summary["completed_per_second"],
                    summary["completed"] * 1e9 / summary["duration_ns"],
                    places=7,
                )
                self.assertLessEqual(summary["peak_queued"], summary["capacity"])
        self.assertEqual(
            seen,
            {
                (p, c, load)
                for p in ["BLOCK", "REJECT"]
                for c in [2, 8, 32]
                for load in ["burst", "paced"]
            },
        )

    def test_task_timestamps_and_synthetic_failures(self):
        for path in RAW.glob("*-tasks.csv"):
            for row in read_rows(path):
                with self.subTest(scenario=row["scenario"], task=row["task_id"]):
                    request, admit, start, finish = (
                        int(row[k])
                        for k in [
                            "requested_ns",
                            "admitted_ns",
                            "started_ns",
                            "finished_ns",
                        ]
                    )
                    self.assertGreaterEqual(request, 0)
                    self.assertGreaterEqual(finish, request)
                    self.assertEqual(int(row["end_to_end_ns"]), finish - request)
                    if row["outcome"] == "REJECTED":
                        self.assertEqual((admit, start), (-1, -1))
                        self.assertEqual(int(row["queue_wait_ns"]), -1)
                        self.assertIn(row["reason"], ["FULL", "TIMEOUT"])
                        admission_end = finish
                    else:
                        self.assertLessEqual(request, admit)
                        self.assertLessEqual(admit, start)
                        self.assertLessEqual(start, finish)
                        self.assertEqual(int(row["queue_wait_ns"]), start - admit)
                        fails = int(row["task_id"]) % 17 == 0
                        self.assertEqual(
                            row["outcome"], "FAILED" if fails else "COMPLETED"
                        )
                        self.assertEqual(
                            row["reason"], "IllegalStateException" if fails else ""
                        )
                        admission_end = admit
                    self.assertEqual(
                        int(row["admission_wait_ns"]), admission_end - request
                    )

    def test_occupancy_bounds_and_final_drain(self):
        for path in RAW.glob("*-occupancy.csv"):
            observations = read_rows(path)
            scenario = observations[0]["scenario"]
            summary = json.loads((RAW / f"{scenario}-summary.json").read_text())
            with self.subTest(scenario=scenario):
                previous = -1
                for row in observations:
                    self.assertGreaterEqual(int(row["time_ns"]), previous)
                    previous = int(row["time_ns"])
                    self.assertIn(int(row["queued"]), range(summary["capacity"] + 1))
                    self.assertIn(int(row["running"]), range(3))
                    self.assertIn(int(row["waiting_producers"]), range(4))
                    self.assertIn(row["state"], ["OPEN", "CLOSING", "TERMINATED"])
                self.assertEqual(observations[-1]["state"], "TERMINATED")
                self.assertEqual(int(observations[-1]["queued"]), 0)
                self.assertEqual(int(observations[-1]["running"]), 0)


if __name__ == "__main__":
    unittest.main()
