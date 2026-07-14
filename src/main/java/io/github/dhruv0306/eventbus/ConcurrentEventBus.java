package io.github.dhruv0306.eventbus;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ConcurrentEventBus implements EventBus {

    private static final Logger LOGGER = Logger.getLogger(ConcurrentEventBus.class.getName());

    // Subscriber registry: event type -> set of filtered subscribers
    private final ConcurrentHashMap<Class<? extends Event>, Set<FilteredSubscriber<?>>> subscribers;
    // Virtual thread executor - one new VT per async task, unbounded creation
    private final ExecutorService asyncExecutor;
    // Global backpressure throttle - limits concurrent handler executions
    private final Semaphore asyncThrottle;
    private final DeadLetterQueue deadLetterQueue;
    // Ensures close() is idempotent and publish() rejects after shutdown
    private final AtomicBoolean closed;

    public ConcurrentEventBus(EventBusConfig config) {
        this.subscribers = new ConcurrentHashMap<>();
        this.asyncExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.asyncThrottle = new Semaphore(config.maxConcurrentDispatches());
        this.deadLetterQueue = new BoundedDeadLetterQueue(config.deadLetterQueueCapacity());
        this.closed = new AtomicBoolean(false);
    }

    public ConcurrentEventBus() {
        this(EventBusConfig.defaults());
    }

    @Override
    public <T extends Event> Subscription subscribe(Class<T> eventType, Consumer<T> handler) {
        return subscribe(eventType, handler, event -> true);
    }

    @Override
    public <T extends Event> Subscription subscribe(Class<T> eventType, Consumer<T> handler, Predicate<T> filter) {
        if (closed.get()) {
            throw new IllegalStateException("EventBus is closed");
        }
        FilteredSubscriber<T> subscriber = new FilteredSubscriber<>(eventType, handler, filter);
        // computeIfAbsent is atomic - safe for concurrent registration
        Set<FilteredSubscriber<?>> eventSubscribers = subscribers.computeIfAbsent(
                eventType, k -> ConcurrentHashMap.newKeySet()
        );
        eventSubscribers.add(subscriber);

        // Return an anonymous Subscription that removes this subscriber on unsubscribe
        return new Subscription() {
            private final AtomicBoolean active = new AtomicBoolean(true);

            @Override
            public void unsubscribe() {
                if (active.compareAndSet(true, false)) {
                    eventSubscribers.remove(subscriber);
                }
            }

            @Override
            public boolean isActive() {
                return active.get();
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Event> void publish(T event) {
        if (closed.get()) {
            throw new IllegalStateException("EventBus is closed");
        }
        Set<FilteredSubscriber<?>> eventSubscribers = subscribers.get(event.getClass());
        // No subscribers - route to DLQ
        if (eventSubscribers == null || eventSubscribers.isEmpty()) {
            boolean accepted = deadLetterQueue.offer(event, new NoSubscriberException(event));
            if (!accepted) {
                throw new EventDroppedException(event,
                        "Dead letter queue is full, event dropped: " + event.getClass().getName());
            }
            return;
        }
        // Synchronous dispatch: deliver to each matching subscriber on the caller's thread
        for (FilteredSubscriber<?> subscriber : eventSubscribers) {
            FilteredSubscriber<T> typed = (FilteredSubscriber<T>) subscriber;
            if (typed.filter().test(event)) {
                try {
                    typed.handler().accept(event);
                } catch (Exception e) {
                    deadLetterQueue.offer(event, e);
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Event> void publishAsync(T event) {
        if (closed.get()) {
            throw new IllegalStateException("EventBus is closed");
        }
        Set<FilteredSubscriber<?>> eventSubscribers = subscribers.get(event.getClass());
        if (eventSubscribers == null || eventSubscribers.isEmpty()) {
            boolean accepted = deadLetterQueue.offer(event, new NoSubscriberException(event));
            if (!accepted) {
                throw new EventDroppedException(event,
                        "Dead letter queue is full, event dropped: " + event.getClass().getName());
            }
            return;
        }
        for (FilteredSubscriber<?> subscriber : eventSubscribers) {
            FilteredSubscriber<T> typed = (FilteredSubscriber<T>) subscriber;
            if (typed.filter().test(event)) {
                // Each matching subscriber gets its own virtual thread
                asyncExecutor.submit(() -> {
                    try {
                        // Acquire permit INSIDE the VT - parks without pinning the carrier
                        asyncThrottle.acquire();
                        try {
                            typed.handler().accept(event);
                        } finally {
                            // Release in finally prevents permit leaks on handler exceptions
                            asyncThrottle.release();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOGGER.log(Level.WARNING, "Async dispatch interrupted for event: " + event, e);
                    } catch (Exception e) {
                        deadLetterQueue.offer(event, e);
                        LOGGER.log(Level.WARNING, "Subscriber threw exception for event: " + event, e);
                    }
                });
            }
        }
    }

    public DeadLetterQueue deadLetterQueue() {
        return deadLetterQueue;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Ordered shutdown: stop accepting -> drain in-flight -> force-stop -> clear
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            subscribers.clear();
        }
    }

    // Record gives us component-wise equals - each lambda registration is unique
    private record FilteredSubscriber<T extends Event>(
            Class<T> eventType,
            Consumer<T> handler,
            Predicate<T> filter
    ) {
    }

    private static class NoSubscriberException extends RuntimeException {
        NoSubscriberException(Event event) {
            super("No subscribers registered for event type: " + event.getClass().getName());
        }
    }
}