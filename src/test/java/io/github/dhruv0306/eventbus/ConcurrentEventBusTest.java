package io.github.dhruv0306.eventbus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentEventBusTest {

    private ConcurrentEventBus bus;

    // Domain event records used across all tests
    record UserCreatedEvent(String username) implements Event {
    }

    record OrderPlacedEvent(String orderId, double amount) implements Event {
    }

    @BeforeEach
    void setUp() {
        // Small config values make failures surface faster in tests
        bus = new ConcurrentEventBus(EventBusConfig.builder()
                .maxConcurrentDispatches(16)
                .deadLetterQueueCapacity(64)
                .build());
    }

    @AfterEach
    void tearDown() {
        bus.close();
    }

    @Test
    void shouldDeliverEventToSubscriber() {
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(UserCreatedEvent.class, event -> received.add(event.username()));

        bus.publish(new UserCreatedEvent("dhruv"));

        assertEquals(1, received.size());
        assertEquals("dhruv", received.getFirst());
    }

    @Test
    void shouldDeliverEventToMultipleSubscribers() {
        List<String> first = new CopyOnWriteArrayList<>();
        List<String> second = new CopyOnWriteArrayList<>();
        bus.subscribe(UserCreatedEvent.class, event -> first.add(event.username()));
        bus.subscribe(UserCreatedEvent.class, event -> second.add(event.username()));

        bus.publish(new UserCreatedEvent("dhruv"));

        assertEquals(1, first.size());
        assertEquals(1, second.size());
    }

    @Test
    void shouldOnlyDeliverToMatchingEventType() {
        List<String> users = new CopyOnWriteArrayList<>();
        List<String> orders = new CopyOnWriteArrayList<>();
        bus.subscribe(UserCreatedEvent.class, event -> users.add(event.username()));
        bus.subscribe(OrderPlacedEvent.class, event -> orders.add(event.orderId()));

        bus.publish(new UserCreatedEvent("dhruv"));

        assertEquals(1, users.size());
        assertEquals(0, orders.size());
    }

    @Test
    void shouldFilterEventsWithPredicate() {
        List<OrderPlacedEvent> highValue = new CopyOnWriteArrayList<>();
        bus.subscribe(OrderPlacedEvent.class, highValue::add, order -> order.amount() > 100.0);

        bus.publish(new OrderPlacedEvent("order-1", 50.0));
        bus.publish(new OrderPlacedEvent("order-2", 200.0));

        assertEquals(1, highValue.size());
        assertEquals("order-2", highValue.getFirst().orderId());
    }

    @Test
    void shouldUnsubscribeSuccessfully() {
        List<String> received = new CopyOnWriteArrayList<>();
        Subscription sub = bus.subscribe(UserCreatedEvent.class, event -> received.add(event.username()));

        bus.publish(new UserCreatedEvent("first"));
        sub.unsubscribe();
        bus.publish(new UserCreatedEvent("second"));

        assertEquals(1, received.size());
        assertFalse(sub.isActive());
    }

    @Test
    void shouldDeliverAsyncEvents() throws InterruptedException {
        // CountDownLatch blocks the test thread until the async handler fires
        CountDownLatch latch = new CountDownLatch(1);
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(UserCreatedEvent.class, event -> {
            received.add(event.username());
            latch.countDown();
        });

        bus.publishAsync(new UserCreatedEvent("async-dhruv"));
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("async-dhruv", received.getFirst());
    }

    @Test
    void shouldRouteUnhandledEventsToDeadLetterQueue() {
        // No subscriber registered for OrderPlacedEvent
        bus.publish(new OrderPlacedEvent("orphan-order", 99.0));

        assertEquals(1, bus.deadLetterQueue().size());
    }

    @Test
    void shouldRouteSubscriberExceptionsToDeadLetterQueue() {
        bus.subscribe(UserCreatedEvent.class, event -> {
            throw new RuntimeException("Handler failure");
        });

        bus.publish(new UserCreatedEvent("failing"));

        assertEquals(1, bus.deadLetterQueue().size());
    }

    @Test
    void shouldThrowWhenDLQIsFull() {
        // Create a bus with a tiny DLQ to force overflow
        ConcurrentEventBus smallBus = new ConcurrentEventBus(EventBusConfig.builder()
                .deadLetterQueueCapacity(2)
                .build());

        smallBus.publish(new UserCreatedEvent("orphan-1"));
        smallBus.publish(new UserCreatedEvent("orphan-2"));

        assertThrows(EventDroppedException.class, () ->
                smallBus.publish(new UserCreatedEvent("orphan-3")));

        smallBus.close();
    }

    @Test
    void shouldHandleConcurrentPublishing() throws InterruptedException {
        List<String> received = new CopyOnWriteArrayList<>();
        bus.subscribe(UserCreatedEvent.class, event -> received.add(event.username()));

        // Launch 100 virtual threads publishing simultaneously
        int threadCount = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            int index = i;
            Thread.ofVirtual().start(() -> {
                bus.publish(new UserCreatedEvent("user-" + index));
                latch.countDown();
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(threadCount, received.size());
    }
}