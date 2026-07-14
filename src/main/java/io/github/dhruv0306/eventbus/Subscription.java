package io.github.dhruv0306.eventbus;

/**
 * Handle returned from EventBus.subscribe() that allows the caller
 * to unsubscribe and check subscription status.
 */
public interface Subscription {

    void unsubscribe();

    boolean isActive();
}