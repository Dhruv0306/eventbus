package io.github.dhruv0306.eventbus;

/**
 * Marker interface for all events dispatched through the EventBus.
 * This interface is intentionally NOT sealed to allow consumers
 * of the library to define their own custom event types.
 */
public interface Event {
}