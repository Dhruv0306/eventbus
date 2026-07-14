package io.github.dhruv0306.eventbus;

/**
 * Thrown when an event cannot be routed (no subscribers) and the dead-letter queue
 * is at full capacity. Indicates the system is under sustained pressure.
 */
public class EventDroppedException extends RuntimeException {

    private final Event droppedEvent;

    public EventDroppedException(Event droppedEvent, String message) {
        super(message);
        this.droppedEvent = droppedEvent;
    }

    public Event getDroppedEvent() {
        return droppedEvent;
    }
}