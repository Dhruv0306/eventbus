package io.github.dhruv0306.eventbus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

// Bounded DLQ backed by ArrayBlockingQueue - returns false on overflow instead of blocking
public final class BoundedDeadLetterQueue implements DeadLetterQueue {

    private final ArrayBlockingQueue<DeadLetter> queue;

    public BoundedDeadLetterQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        // Fixed-size queue enforces the memory ceiling
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public boolean offer(Event event, Throwable reason) {
        // Wrap the event with metadata (reason + timestamp) for diagnostics
        DeadLetter deadLetter = new DeadLetter(event, reason, System.currentTimeMillis());
        // Non-blocking offer: returns false if full, letting the caller decide policy
        return queue.offer(deadLetter);
    }

    @Override
    public List<DeadLetter> drain() {
        // drainTo is atomic - moves all elements in one operation
        List<DeadLetter> drained = new ArrayList<>();
        queue.drainTo(drained);
        return drained;
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isFull() {
        return queue.remainingCapacity() == 0;
    }
}