package io.github.dhruv0306.eventbus;

import java.util.List;

/**
 * Interface for a dead-letter queue that captures undeliverable events
 * or events whose handlers threw exceptions.
 */
public interface DeadLetterQueue {

    boolean offer(Event event, Throwable reason);

    List<DeadLetter> drain();

    int size();

    boolean isFull();

    record DeadLetter(Event event, Throwable reason, long timestamp) {
    }
}