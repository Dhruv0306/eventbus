package io.github.dhruv0306.eventbus;

/**
 * Configuration for the ConcurrentEventBus, built via the Builder pattern.
 * Controls backpressure limits and dead-letter queue capacity.
 */
public final class EventBusConfig {

    private final int maxConcurrentDispatches;
    private final int deadLetterQueueCapacity;

    private EventBusConfig(Builder builder) {
        this.maxConcurrentDispatches = builder.maxConcurrentDispatches;
        this.deadLetterQueueCapacity = builder.deadLetterQueueCapacity;
    }

    public int maxConcurrentDispatches() {
        return maxConcurrentDispatches;
    }

    public int deadLetterQueueCapacity() {
        return deadLetterQueueCapacity;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static EventBusConfig defaults() {
        return new Builder().build();
    }

    public static final class Builder {
        private int maxConcurrentDispatches = 256;
        private int deadLetterQueueCapacity = 1024;

        private Builder() {
        }

        public Builder maxConcurrentDispatches(int maxConcurrentDispatches) {
            if (maxConcurrentDispatches <= 0) {
                throw new IllegalArgumentException("maxConcurrentDispatches must be positive");
            }
            this.maxConcurrentDispatches = maxConcurrentDispatches;
            return this;
        }

        public Builder deadLetterQueueCapacity(int deadLetterQueueCapacity) {
            if (deadLetterQueueCapacity <= 0) {
                throw new IllegalArgumentException("deadLetterQueueCapacity must be positive");
            }
            this.deadLetterQueueCapacity = deadLetterQueueCapacity;
            return this;
        }

        public EventBusConfig build() {
            return new EventBusConfig(this);
        }
    }
}