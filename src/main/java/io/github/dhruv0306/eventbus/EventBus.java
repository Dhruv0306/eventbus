package io.github.dhruv0306.eventbus;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Public API for the in-memory event bus.
 * Supports synchronous and asynchronous dispatching with type-safe subscriptions.
 */
public interface EventBus extends AutoCloseable {

    <T extends Event> Subscription subscribe(Class<T> eventType, Consumer<T> handler);

    <T extends Event> Subscription subscribe(Class<T> eventType, Consumer<T> handler, Predicate<T> filter);

    <T extends Event> void publish(T event);

    <T extends Event> void publishAsync(T event);

    @Override
    void close();
}