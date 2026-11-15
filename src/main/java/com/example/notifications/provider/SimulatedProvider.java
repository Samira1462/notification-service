package com.example.notifications.provider;

import com.example.notifications.config.NotificationProperties;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stand-in for a real provider SDK: validates the recipient (permanent failure), simulates latency and
 * randomly fails with a retryable error. Replace the {@code deliver} step with a real client call.
 */
public abstract class SimulatedProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(SimulatedProvider.class);

    private final double failureRate;
    private final Duration latency;

    protected SimulatedProvider(NotificationProperties properties) {
        this.failureRate = properties.providers().simulatedFailureRate();
        this.latency = properties.providers().simulatedLatency();
    }

    @Override
    public final void send(String idempotencyKey, String recipient, String subject, String body) {
        validateRecipient(recipient);
        simulateNetworkCall();
        // Avoid logging the recipient or body in real systems: both are personal data.
        log.info("Delivered {} notification (idempotencyKey={})", channel(), idempotencyKey);
    }

    /** @throws NonRetryableNotificationException if the recipient can never be valid */
    protected abstract void validateRecipient(String recipient);

    private void simulateNetworkCall() {
        try {
            if (!latency.isZero()) {
                Thread.sleep(latency.toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableNotificationException(channel() + " call interrupted", e);
        }
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new RetryableNotificationException(channel() + " provider timeout (simulated)");
        }
    }
}
