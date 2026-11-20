package lab;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Actual Java-thread workload runs; no synthetic timing values are generated. */
public final class PipelineStudy {
    private static volatile long workSink;
    private record Observation(long ns, BoundedPipeline.Snapshot snapshot) {}

    private static void delay(long nanos) {
        long deadline = System.nanoTime() + nanos;
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            if (Thread.currentThread().isInterrupted()) return;
            LockSupport.parkNanos(remaining);
        }
    }

    private static void service(long id) {
        long value = id + 1;
        for (int i = 0; i < 12000; i++) {
            value ^= value << 13;
            value ^= value >>> 7;
            value ^= value << 17;
        }
        workSink = value;
        delay(2_000_000); // Models blocking service time; no network or disk I/O.
        if (id % 17 == 0) {
            throw new IllegalStateException("Synthetic task failure");
        }
    }

    private static String q(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void run(Path output, BoundedPipeline.Policy policy, int capacity,
                            String load, int tasksPerProducer) throws Exception {
        String scenario = policy.name().toLowerCase(Locale.ROOT) + "-c" + capacity + "-" + load;
        var pipeline = new BoundedPipeline(2, capacity, policy);
        var observations = new ArrayList<Observation>();
        var observe = new AtomicBoolean(true);
        var threadFailure = new AtomicReference<Throwable>();
        Thread sampler = new Thread(() -> {
            try {
                while (observe.get()) {
                    observations.add(new Observation(pipeline.elapsedNanos(), pipeline.snapshot()));
                    LockSupport.parkNanos(500_000);
                }
                observations.add(new Observation(pipeline.elapsedNanos(), pipeline.snapshot()));
            } catch (Throwable failure) {
                threadFailure.compareAndSet(null, failure);
            }
        }, "occupancy-sampler");
        var nextId = new AtomicLong();
        var ready = new CountDownLatch(3);
        var start = new CountDownLatch(1);
        List<Thread> producers = new ArrayList<>();
        long startNs;
        long finishNs;
        try {
            sampler.start();
            for (int producerId = 0; producerId < 3; producerId++) {
                Thread producer = new Thread(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < tasksPerProducer; i++) {
                            if (Thread.currentThread().isInterrupted()) return;
                            long id = nextId.getAndIncrement();
                            pipeline.submit(id, () -> service(id), Duration.ofMillis(3));
                            if (load.equals("paced")) {
                                delay(4_000_000);
                            }
                        }
                    } catch (InterruptedException interrupted) {
                        threadFailure.compareAndSet(null, interrupted);
                        Thread.currentThread().interrupt();
                    } catch (Throwable failure) {
                        threadFailure.compareAndSet(null, failure);
                    }
                }, "producer-" + producerId);
                producers.add(producer);
                producer.start();
            }
            if (!ready.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Producers failed to start");
            }
            startNs = pipeline.elapsedNanos();
            start.countDown();
            long joinDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            for (Thread producer : producers) {
                joinBefore(producer, joinDeadline);
            }
            if (threadFailure.get() != null) {
                throw new IllegalStateException("Study thread failed", threadFailure.get());
            }
            pipeline.close();
            if (!pipeline.awaitTermination(Duration.ofSeconds(20))) {
                throw new IllegalStateException("Scenario failed to drain");
            }
            finishNs = pipeline.elapsedNanos();
        } finally {
            // Always wake a producer awaiting release and close ownership, even if
            // startup, a join, or the workload failed. The finite service is not killed.
            pipeline.close();
            start.countDown();
            boolean interrupted = Thread.interrupted();
            for (Thread producer : producers) {
                if (producer.isAlive()) producer.interrupt();
            }
            observe.set(false);
            sampler.interrupt();
            List<Thread> cleanupThreads = new ArrayList<>(producers);
            cleanupThreads.add(sampler);
            long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            try {
                for (Thread thread : cleanupThreads) {
                    while (thread.isAlive()) {
                        try {
                            joinBefore(thread, cleanupDeadline);
                        } catch (InterruptedException interruptedJoin) {
                            interrupted = true;
                        }
                    }
                }
            } finally {
                // Clear an interrupt during cleanup, then restore it for our caller.
                interrupted |= Thread.interrupted();
                pipeline.awaitTermination(Duration.ofSeconds(20));
                interrupted |= Thread.interrupted();
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
        if (threadFailure.get() != null) {
            throw new IllegalStateException("Study thread failed", threadFailure.get());
        }
        var snapshot = pipeline.snapshot();
        if (!snapshot.accountsBalance() || snapshot.attempted() != 3L * tasksPerProducer) {
            throw new IllegalStateException("Scenario task accounting failed");
        }
        StringBuilder tasks = new StringBuilder("scenario,task_id,requested_ns,admitted_ns,started_ns,finished_ns,outcome,reason,admission_wait_ns,queue_wait_ns,end_to_end_ns\n");
        for (var task : pipeline.results()) {
            long admissionEnd = task.admitted() ? task.admittedNs() : task.finishedNs();
            long queueWait = task.startedNs() >= 0 ? task.startedNs() - task.admittedNs() : -1;
            tasks.append(scenario).append(',').append(task.id()).append(',')
                .append(task.requestedNs()).append(',').append(task.admittedNs()).append(',')
                .append(task.startedNs()).append(',').append(task.finishedNs()).append(',')
                .append(task.outcome()).append(',').append(task.reason()).append(',')
                .append(admissionEnd - task.requestedNs()).append(',').append(queueWait).append(',')
                .append(task.finishedNs() - task.requestedNs()).append('\n');
        }
        Files.writeString(output.resolve(scenario + "-tasks.csv"), tasks, StandardCharsets.UTF_8);
        StringBuilder occupancy = new StringBuilder("scenario,time_ns,queued,running,waiting_producers,state\n");
        for (var observation : observations) {
            var s = observation.snapshot();
            if (!s.accountsBalance() || s.queued() > capacity) throw new AssertionError("Invalid sampled state");
            occupancy.append(scenario).append(',').append(observation.ns()).append(',')
                .append(s.queued()).append(',').append(s.running()).append(',')
                .append(s.waitingProducers()).append(',').append(s.state()).append('\n');
        }
        Files.writeString(output.resolve(scenario + "-occupancy.csv"), occupancy, StandardCharsets.UTF_8);
        String summary = String.format(Locale.ROOT,
            "{\n  \"scenario\": %s,\n  \"policy\": %s,\n  \"capacity\": %d,\n  \"load\": %s,\n" +
            "  \"workers\": 2,\n  \"producers\": 3,\n  \"attempted\": %d,\n  \"admitted\": %d,\n" +
            "  \"rejected\": %d,\n  \"completed\": %d,\n  \"failed\": %d,\n  \"cancelled\": %d,\n" +
            "  \"peak_queued\": %d,\n  \"start_ns\": %d,\n  \"finish_ns\": %d,\n  \"duration_ns\": %d,\n" +
            "  \"completed_per_second\": %.9f\n}\n",
            q(scenario), q(policy.name()), capacity, q(load), snapshot.attempted(), snapshot.admitted(),
            snapshot.rejected(), snapshot.completed(), snapshot.failed(), snapshot.cancelled(), snapshot.peakQueued(),
            startNs, finishNs, finishNs - startNs, snapshot.completed() * 1e9 / (finishNs - startNs));
        Files.writeString(output.resolve(scenario + "-summary.json"), summary, StandardCharsets.UTF_8);
        System.out.println(scenario + " admitted=" + snapshot.admitted() + " rejected=" + snapshot.rejected()
            + " completed=" + snapshot.completed() + " failed=" + snapshot.failed());
    }

    private static void joinBefore(Thread thread, long deadline) throws InterruptedException {
        while (thread.isAlive()) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) throw new IllegalStateException("Thread did not stop: " + thread.getName());
            TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        }
    }

    public static void main(String[] args) throws Exception {
        Path output = Path.of(args.length > 0 ? args[0] : "results/raw");
        int tasks = args.length > 1 ? Integer.parseInt(args[1]) : 60;
        if (tasks < 1 || tasks > 1000) throw new IllegalArgumentException("Tasks per producer must be 1..1000");
        Files.createDirectories(output);
        for (var policy : BoundedPipeline.Policy.values()) {
            for (int capacity : new int[] {2, 8, 32}) {
                for (String load : new String[] {"burst", "paced"}) {
                    run(output, policy, capacity, load, tasks);
                }
            }
        }
        String environment = "{\n  \"recorded_at_utc\": " + q(Instant.now().toString())
            + ",\n  \"java_version\": " + q(System.getProperty("java.version"))
            + ",\n  \"java_runtime\": " + q(System.getProperty("java.runtime.name"))
            + ",\n  \"java_vendor\": " + q(System.getProperty("java.vendor"))
            + ",\n  \"os\": " + q(System.getProperty("os.name"))
            + ",\n  \"architecture\": " + q(System.getProperty("os.arch"))
            + ",\n  \"available_processors\": " + Runtime.getRuntime().availableProcessors()
            + ",\n  \"tasks_per_producer\": " + tasks
            + ",\n  \"queue_wait_budget_ms\": 3,\n  \"nominal_service_delay_ms\": 2,\n"
            + "  \"paced_producer_delay_ms\": 4,\n  \"occupancy_nominal_interval_ms\": 0.5\n}\n";
        Files.writeString(output.resolve("environment.json"), environment, StandardCharsets.UTF_8);
    }
}
