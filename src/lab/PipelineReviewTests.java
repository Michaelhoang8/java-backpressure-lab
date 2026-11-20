package lab;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Independent ownership and admission boundary tests. */
public final class PipelineReviewTests {
    private static final Duration LIMIT = Duration.ofSeconds(5);

    private static void check(boolean value, String message) {
        if (!value) throw new AssertionError(message);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        check(latch.await(5, TimeUnit.SECONDS), "Latch timeout");
    }

    private static void duplicateIdsHaveOneOwner() throws Exception {
        var pipeline = new BoundedPipeline(2, 4, BoundedPipeline.Policy.BLOCK);
        var gate = new CountDownLatch(1);
        var ready = new CountDownLatch(8);
        var accepted = new AtomicInteger();
        var duplicates = new AtomicInteger();
        var executions = new AtomicInteger();
        var unexpected = new AtomicReference<Throwable>();
        var producers = new ArrayList<Thread>();
        try {
            for (int i = 0; i < 8; i++) {
                Thread producer = new Thread(() -> {
                    ready.countDown();
                    try {
                        gate.await();
                        if (pipeline.submit(42, executions::incrementAndGet, LIMIT)) accepted.incrementAndGet();
                    } catch (IllegalArgumentException duplicate) {
                        duplicates.incrementAndGet();
                    } catch (Throwable error) {
                        unexpected.compareAndSet(null, error);
                    }
                });
                producers.add(producer);
                producer.start();
            }
            await(ready);
            gate.countDown();
            for (Thread producer : producers) {
                producer.join(5000);
                check(!producer.isAlive(), "Producer did not terminate");
            }
            check(unexpected.get() == null, "Unexpected producer exception: " + unexpected.get());
            check(accepted.get() == 1 && duplicates.get() == 7, "Task ID did not have one owner");
        } finally {
            gate.countDown();
            pipeline.close();
            check(pipeline.awaitTermination(LIMIT), "Drain timeout");
        }
        check(executions.get() == 1, "Duplicate task executed");
        check(pipeline.snapshot().attempted() == 1, "Invalid duplicate IDs entered accounting");
        check(pipeline.snapshot().accountsBalance(), "Accounting mismatch");
        System.out.println("PASS concurrent_duplicate_id_ownership");
    }

    private static void cancellationReleasesProducerAndTimeoutRejects() throws Exception {
        var pipeline = new BoundedPipeline(1, 1, BoundedPipeline.Policy.BLOCK);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var admitted = new AtomicInteger();
        Thread producer = null;
        try {
            check(pipeline.submit(1, () -> { started.countDown(); release.await(); }, LIMIT), "First admission");
            await(started);
            check(pipeline.submit(2, () -> { throw new AssertionError("Cancelled task executed"); }, LIMIT), "Second admission");
            check(!pipeline.submit(3, () -> {}, Duration.ofMillis(20)), "Positive wait budget did not expire");
            check(pipeline.results().stream().anyMatch(r -> r.id() == 3 && r.reason().equals("TIMEOUT")), "Missing timeout outcome");
            producer = new Thread(() -> {
                if (pipeline.submit(4, () -> {}, LIMIT)) admitted.incrementAndGet();
            });
            producer.start();
            check(pipeline.awaitWaitingProducers(1, LIMIT), "Producer never waited for capacity");
            check(pipeline.cancelQueued(2), "Cancellation failed");
            producer.join(5000);
            check(!producer.isAlive() && admitted.get() == 1, "Cancellation did not release blocked admission");
            check(pipeline.snapshot().queued() == 1 && pipeline.snapshot().running() == 1, "Unexpected ownership before release");
        } finally {
            pipeline.close();
            release.countDown();
            if (producer != null) producer.join(5000);
            check(pipeline.awaitTermination(LIMIT), "Drain timeout");
        }
        var summary = pipeline.snapshot();
        check(summary.completed() == 2 && summary.cancelled() == 1 && summary.rejected() == 1, "Wrong terminal outcomes");
        check(summary.accountsBalance(), "Accounting mismatch");
        for (var result : pipeline.results()) {
            if (result.outcome() == BoundedPipeline.Outcome.COMPLETED) {
                check(result.requestedNs() <= result.admittedNs()
                    && result.admittedNs() <= result.startedNs()
                    && result.startedNs() <= result.finishedNs(), "Non-monotone task lifecycle");
            }
        }
        System.out.println("PASS cancellation_wakes_producer_and_positive_timeout");
    }

    public static void main(String[] args) throws Exception {
        duplicateIdsHaveOneOwner();
        cancellationReleasesProducerAndTimeoutRejects();
        System.out.println("ALL 2 INDEPENDENT REVIEW TESTS PASSED");
    }
}
