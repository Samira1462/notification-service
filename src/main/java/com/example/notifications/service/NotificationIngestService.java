package com.example.notifications.service;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationEvent;
import com.example.notifications.producer.NotificationPublisher;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.stereotype.Service;

/**
 * Producer-side entry point: records the event as PENDING, publishes it and waits for the broker ack.
 * The PENDING row gives support immediate visibility, and a client retrying with the same event id is
 * answered as a duplicate instead of sending a second notification.
 */
@Service
public class NotificationIngestService {

    private final NotificationTracker tracker;
    private final NotificationPublisher publisher;
    private final NotificationProperties properties;

    public NotificationIngestService(
            NotificationTracker tracker, NotificationPublisher publisher, NotificationProperties properties) {
        this.tracker = tracker;
        this.publisher = publisher;
        this.properties = properties;
    }

    /** @return {@code true} if the event was published, {@code false} if it was a duplicate */
    public boolean submit(NotificationEvent event) {
        if (tracker.accept(event) == NotificationTracker.Acceptance.DUPLICATE) {
            return false;
        }
        try {
            publishAndWait(event);
        } catch (RuntimeException e) {
            tracker.discardPending(event.eventId());
            throw e;
        }
        return true;
    }

    /** Republishes a FAILED notification (after the cause was fixed) under its original event id. */
    public NotificationEvent replay(String eventId) {
        NotificationEvent event = tracker.prepareReplay(eventId);
        try {
            publishAndWait(event);
        } catch (RuntimeException e) {
            tracker.markFailed(eventId, "Replay could not be published: " + e.getMessage());
            throw e;
        }
        return event;
    }

    private void publishAndWait(NotificationEvent event) {
        try {
            publisher.publish(event).get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishFailedException("Interrupted while publishing event " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new PublishFailedException("Could not publish event " + event.eventId(), e);
        }
    }
}
