package lab;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Dependency-free concurrency tests: latches/condition state establish ordering. */
public final class PipelineTests {
    private static final Duration LIMIT = Duration.ofSeconds(5);
    private static int passed;

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        check(latch.await(5, TimeUnit.SECONDS), "Latch did not reach the expected state");
    }

    private static void join(Thread thread) throws InterruptedException {
        thread.join(5000);
        check(!thread.isAlive(), "Thread did not terminate");
    }

    private static void balanced(BoundedPipeline pipeline) {
        var snapshot = pipeline.snapshot();
        check(snapshot.accountsBalance(), "Task accounting does not balance: " + snapshot);
        check(snapshot.state() == BoundedPipeline.State.TERMINATED, "Workers did not terminate");
    }

    private static void done(String name) {
        passed++;
        System.out.println("PASS " + name);
    }

    private static void rejectSaturatedQueue() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.REJECT);
        try {
            check(pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT), "First admission");
            await(started);
            check(pipeline.submit(2, () -> {}, LIMIT), "Queue slot admission");
            check(!pipeline.submit(3, () -> {}, LIMIT), "Full queue must reject");
            check(pipeline.snapshot().peakQueued() == 1, "Queue capacity exceeded");
        } finally {
            pipeline.close(); release.countDown();
            check(pipeline.awaitTermination(LIMIT), "Drain failed");
        }
        balanced(pipeline);
        check(pipeline.snapshot().completed() == 2 && pipeline.snapshot().rejected() == 1, "Counts");
        done("reject_saturated_queue");
    }

    private static void blockingAdmissionResumes() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var accepted = new AtomicBoolean();
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.BLOCK);
        Thread producer = null;
        try {
            pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT);
            await(started);
            pipeline.submit(2, () -> {}, LIMIT);
            producer = new Thread(() -> accepted.set(pipeline.submit(3, () -> {}, LIMIT)));
            producer.start();
            check(pipeline.awaitWaitingProducers(1, LIMIT), "Producer did not block");
            check(pipeline.snapshot().accountsBalance(), "Waiting ownership accounting");
            release.countDown();
            join(producer);
            check(accepted.get(), "Producer should be admitted after space opens");
        } finally {
            release.countDown(); pipeline.close();
            if (producer != null) join(producer);
            check(pipeline.awaitTermination(LIMIT), "Drain failed");
        }
        balanced(pipeline);
        check(pipeline.snapshot().completed() == 3, "All admitted work must finish");
        done("blocking_admission_resumes");
    }

    private static void closeWakesBlockedProducer() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var accepted = new AtomicBoolean(true);
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.BLOCK);
        Thread producer = null;
        try {
            pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT);
            await(started); pipeline.submit(2, () -> {}, LIMIT);
            producer = new Thread(() -> accepted.set(pipeline.submit(3, () -> {}, LIMIT)));
            producer.start();
            check(pipeline.awaitWaitingProducers(1, LIMIT), "Producer did not block");
            pipeline.close();
            join(producer);
            check(!accepted.get(), "Blocked attempt admitted after close");
            check(!pipeline.submit(4, () -> {}, LIMIT), "New attempt admitted after close");
            check(pipeline.results().stream().filter(r -> r.id() >= 3).allMatch(r -> r.reason().equals("CLOSED")), "Close reason");
        } finally {
            release.countDown(); pipeline.close();
            if (producer != null) join(producer);
            check(pipeline.awaitTermination(LIMIT), "Drain failed");
        }
        balanced(pipeline);
        done("close_wakes_producers_and_drains");
    }

    private static void interruptRestored() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var restored = new AtomicBoolean();
        var rejected = new AtomicBoolean();
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.BLOCK);
        Thread producer = null;
        try {
            pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT);
            await(started); pipeline.submit(2, () -> {}, LIMIT);
            producer = new Thread(() -> {
                rejected.set(!pipeline.submit(3, () -> {}, LIMIT));
                restored.set(Thread.currentThread().isInterrupted());
            });
            producer.start();
            check(pipeline.awaitWaitingProducers(1, LIMIT), "Producer did not block");
            producer.interrupt(); join(producer);
            check(rejected.get() && restored.get(), "Submission interrupt was lost");
            Thread.currentThread().interrupt();
            check(!pipeline.awaitTermination(LIMIT), "Interrupted termination wait should stop");
            check(Thread.currentThread().isInterrupted(), "Termination wait lost interrupt");
            Thread.interrupted();
        } finally {
            Thread.interrupted(); release.countDown(); pipeline.close();
            if (producer != null) join(producer);
            check(pipeline.awaitTermination(LIMIT), "Drain failed");
        }
        balanced(pipeline);
        done("caller_interrupt_restoration");
    }

    private static void timeoutAndCancellation() throws Exception {
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.BLOCK);
        try {
            pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT);
            await(started); pipeline.submit(2, () -> { throw new AssertionError("Cancelled work ran"); }, LIMIT);
            check(!pipeline.submit(3, () -> {}, Duration.ZERO), "Zero queue-wait budget should reject");
            check(pipeline.cancelQueued(2), "Queued cancellation failed");
            check(!pipeline.cancelQueued(2) && !pipeline.cancelQueued(1), "Cancellation must be single/queued only");
            check(pipeline.submit(4, () -> {}, Duration.ZERO), "Cancelled queue slot not released");
        } finally {
            release.countDown(); pipeline.close(); check(pipeline.awaitTermination(LIMIT), "Drain failed");
        }
        balanced(pipeline);
        check(pipeline.snapshot().cancelled() == 1 && pipeline.snapshot().completed() == 2, "Cancellation counts");
        done("timeout_and_queued_cancellation");
    }

    private static void failuresDoNotKillWorker() {
        var pipeline = new BoundedPipeline(1, 8, BoundedPipeline.Policy.BLOCK);
        var cleanInterrupt = new AtomicBoolean();
        pipeline.submit(1, () -> { throw new IllegalStateException("Expected failure"); }, LIMIT);
        pipeline.submit(2, () -> { throw new AssertionError("Expected assertion failure"); }, LIMIT);
        pipeline.submit(3, () -> Thread.currentThread().interrupt(), LIMIT);
        pipeline.submit(4, () -> cleanInterrupt.set(!Thread.currentThread().isInterrupted()), LIMIT);
        pipeline.close(); check(pipeline.awaitTermination(LIMIT), "Worker stopped draining after a task failure");
        balanced(pipeline);
        check(pipeline.snapshot().failed() == 2 && pipeline.snapshot().completed() == 2, "Failure accounting");
        check(cleanInterrupt.get(), "Task interrupt leaked into next task");
        done("task_failure_and_interrupt_isolation");
    }

    private static void closeSubmissionRace() throws Exception {
        for (int round = 0; round < 40; round++) {
            var gate = new CountDownLatch(1);
            var ready = new CountDownLatch(2);
            var closed = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var pipeline = new BoundedPipeline(2, 4, BoundedPipeline.Policy.BLOCK);
            Thread producer = new Thread(() -> {
                ready.countDown();
                try {
                    gate.await();
                    for (int id = 0; id < 30; id++) {
                        boolean alreadyClosed = closed.get();
                        boolean admitted = pipeline.submit(id, () -> {}, Duration.ofMillis(20));
                        check(!alreadyClosed || !admitted, "Admission after close returned");
                    }
                } catch (Throwable thrown) { failure.set(thrown); }
            });
            Thread closer = new Thread(() -> {
                ready.countDown();
                try { gate.await(); pipeline.close(); closed.set(true); }
                catch (Throwable thrown) { failure.set(thrown); }
            });
            producer.start(); closer.start(); await(ready); gate.countDown();
            join(producer); join(closer); pipeline.close();
            check(pipeline.awaitTermination(LIMIT), "Race drain failed");
            if (failure.get() != null) throw new AssertionError("Race failure", failure.get());
            balanced(pipeline);
            check(pipeline.snapshot().attempted() == 30, "An attempted task disappeared");
        }
        done("close_submit_race_40_rounds");
    }

    public static void main(String[] args) throws Exception {
        rejectSaturatedQueue();
        blockingAdmissionResumes();
        closeWakesBlockedProducer();
        interruptRestored();
        timeoutAndCancellation();
        failuresDoNotKillWorker();
        closeSubmissionRace();
        System.out.println("ALL " + passed + " TESTS PASSED");
    }
}
