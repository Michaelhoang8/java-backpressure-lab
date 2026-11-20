package lab;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** A bounded waiting queue with observable admission and draining shutdown semantics. */
public final class BoundedPipeline implements AutoCloseable {
    public enum Policy { BLOCK, REJECT }
    public enum State { OPEN, CLOSING, TERMINATED }
    public enum Outcome { WAITING, QUEUED, RUNNING, COMPLETED, FAILED, CANCELLED, REJECTED }

    @FunctionalInterface
    public interface Work {
        void run() throws Exception;
    }

    public record TaskResult(long id, long requestedNs, long admittedNs, long startedNs,
                             long finishedNs, Outcome outcome, String reason) {
        public boolean admitted() { return admittedNs >= 0; }
    }

    public record Snapshot(State state, int queued, int running, int waitingProducers,
                           int liveWorkers, long attempted, long admitted, long rejected,
                           long completed, long failed, long cancelled, int peakQueued) {
        public boolean accountsBalance() {
            return attempted == admitted + rejected + waitingProducers
                && admitted == queued + running + completed + failed + cancelled;
        }
    }

    private static final class Task {
        final long id;
        final long requestedNs;
        Work work;
        long admittedNs = -1;
        long startedNs = -1;
        long finishedNs = -1;
        Outcome outcome = Outcome.WAITING;
        String reason = "";

        Task(long id, Work work, long requestedNs) {
            this.id = id;
            this.work = work;
            this.requestedNs = requestedNs;
        }

        TaskResult result() {
            return new TaskResult(id, requestedNs, admittedNs, startedNs, finishedNs, outcome, reason);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();
    private final Condition space = lock.newCondition();
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Task> queue = new ArrayDeque<>();
    private final Map<Long, Task> tasks = new LinkedHashMap<>();
    private final int capacity;
    private final Policy policy;
    private final long originNs = System.nanoTime();
    private State state = State.OPEN;
    private int liveWorkers;
    private int running;
    private int waitingProducers;
    private int peakQueued;
    private long admitted;
    private long rejected;
    private long completed;
    private long failed;
    private long cancelled;

    public BoundedPipeline(int workers, int capacity, Policy policy) {
        if (workers < 1 || capacity < 1 || policy == null) {
            throw new IllegalArgumentException("Positive worker count/capacity and a policy are required");
        }
        this.capacity = capacity;
        this.policy = policy;
        this.liveWorkers = workers;
        for (int i = 0; i < workers; i++) {
            Thread worker = new Thread(this::workerLoop, "pipeline-worker-" + i);
            worker.start();
        }
    }

    private long now() { return System.nanoTime() - originNs; }

    public long elapsedNanos() { return now(); }

    /**
     * Returns true iff ownership was admitted to the queue. BLOCK limits only the
     * queue-space condition wait; it is not an end-to-end deadline or lock-acquisition
     * timeout. Interrupted calls reject the attempt and preserve the interrupt flag.
     */
    public boolean submit(long id, Work work, Duration maximumQueueWait) {
        if (id < 0 || work == null || maximumQueueWait == null || maximumQueueWait.isNegative()) {
            throw new IllegalArgumentException("Nonnegative ID/wait and a work item are required");
        }
        long remainingNs = maximumQueueWait.toNanos();
        long requestNs = now();
        lock.lock();
        try {
            if (tasks.containsKey(id)) {
                throw new IllegalArgumentException("Duplicate task ID: " + id);
            }
            Task task = new Task(id, work, requestNs);
            tasks.put(id, task);
            if (Thread.currentThread().isInterrupted()) {
                reject(task, "INTERRUPTED");
                return false;
            }
            while (state == State.OPEN && queue.size() >= capacity) {
                if (policy == Policy.REJECT) {
                    reject(task, "FULL");
                    return false;
                }
                if (remainingNs <= 0) {
                    reject(task, "TIMEOUT");
                    return false;
                }
                waitingProducers++;
                changed.signalAll();
                try {
                    remainingNs = space.awaitNanos(remainingNs);
                } catch (InterruptedException interrupted) {
                    reject(task, "INTERRUPTED");
                    Thread.currentThread().interrupt();
                    return false;
                } finally {
                    waitingProducers--;
                    changed.signalAll();
                }
            }
            if (state != State.OPEN) {
                reject(task, "CLOSED");
                return false;
            }
            task.admittedNs = now();
            task.outcome = Outcome.QUEUED;
            queue.addLast(task);
            admitted++;
            peakQueued = Math.max(peakQueued, queue.size());
            available.signal();
            changed.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void reject(Task task, String reason) {
        task.outcome = Outcome.REJECTED;
        task.reason = reason;
        task.finishedNs = now();
        task.work = null;
        rejected++;
        changed.signalAll();
    }

    /** Cancel only a task still waiting in the queue; running work is not interrupted. */
    public boolean cancelQueued(long id) {
        lock.lock();
        try {
            Task task = tasks.get(id);
            if (task == null || task.outcome != Outcome.QUEUED) {
                return false;
            }
            if (!queue.remove(task)) {
                throw new IllegalStateException("Queued task is absent from queue");
            }
            task.outcome = Outcome.CANCELLED;
            task.reason = "CANCELLED_BEFORE_START";
            task.finishedNs = now();
            task.work = null;
            cancelled++;
            space.signalAll();
            changed.signalAll();
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void workerLoop() {
        try {
            while (true) {
                Task task;
                Work work;
                lock.lock();
                try {
                    // Threads are owned by this pipeline; shutdown uses state and
                    // signals, not interrupts. A task may leave its worker interrupted.
                    while (queue.isEmpty() && state == State.OPEN) {
                        available.awaitUninterruptibly();
                    }
                    if (queue.isEmpty()) {
                        return;
                    }
                    task = queue.removeFirst();
                    task.startedNs = now();
                    task.outcome = Outcome.RUNNING;
                    running++;
                    work = task.work;
                    task.work = null;
                    space.signalAll();
                    changed.signalAll();
                } finally {
                    lock.unlock();
                }

                Throwable failure = null;
                // Isolate one task's leftover interrupt from the next task.
                Thread.interrupted();
                try {
                    work.run();
                } catch (Throwable thrown) {
                    failure = thrown;
                } finally {
                    lock.lock();
                    try {
                        running--;
                        task.finishedNs = now();
                        if (failure == null) {
                            task.outcome = Outcome.COMPLETED;
                            completed++;
                        } else {
                            task.outcome = Outcome.FAILED;
                            task.reason = failure.getClass().getSimpleName();
                            failed++;
                        }
                        changed.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } finally {
            lock.lock();
            try {
                liveWorkers--;
                if (liveWorkers == 0) {
                    state = State.TERMINATED;
                }
                changed.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /** Begin graceful shutdown: reject new ownership and drain already admitted work. */
    @Override
    public void close() {
        lock.lock();
        try {
            if (state == State.OPEN) {
                state = State.CLOSING;
                available.signalAll();
                space.signalAll();
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Wait for worker exit; false means timeout or interruption, not forced cancellation. */
    public boolean awaitTermination(Duration timeout) {
        if (timeout == null || timeout.isNegative()) {
            throw new IllegalArgumentException("Nonnegative timeout required");
        }
        long remaining = timeout.toNanos();
        lock.lock();
        try {
            while (state != State.TERMINATED) {
                if (remaining <= 0) {
                    return false;
                }
                try {
                    remaining = changed.awaitNanos(remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** State-based diagnostic wait, used to establish saturation without sleep assertions. */
    public boolean awaitWaitingProducers(int minimum, Duration timeout) throws InterruptedException {
        if (minimum < 0 || timeout.isNegative()) {
            throw new IllegalArgumentException("Nonnegative minimum and timeout required");
        }
        long remaining = timeout.toNanos();
        lock.lockInterruptibly();
        try {
            while (waitingProducers < minimum) {
                if (remaining <= 0) {
                    return false;
                }
                remaining = changed.awaitNanos(remaining);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public Snapshot snapshot() {
        lock.lock();
        try {
            return new Snapshot(state, queue.size(), running, waitingProducers, liveWorkers,
                tasks.size(), admitted, rejected, completed, failed, cancelled, peakQueued);
        } finally {
            lock.unlock();
        }
    }

    public List<TaskResult> results() {
        lock.lock();
        try {
            List<TaskResult> result = new ArrayList<>();
            for (Task task : tasks.values()) {
                result.add(task.result());
            }
            return List.copyOf(result);
        } finally {
            lock.unlock();
        }
    }
}
