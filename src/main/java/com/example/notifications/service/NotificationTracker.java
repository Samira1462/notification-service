package com.example.notifications.service;

import com.example.notifications.domain.NotificationRecord;
import com.example.notifications.domain.NotificationRecordRepository;
import com.example.notifications.model.NotificationEvent;
import com.example.notifications.model.NotificationStatus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Owns the {@link NotificationRecord} lifecycle: duplicate detection, status transitions and replay.
 *
 * <p>Delivery is at-least-once. A crash after the provider accepted a message but before
 * {@link #markSent} commits will cause a resend on redelivery; the provider idempotency key passed by
 * {@code NotificationService} is the second line of defence for that window.
 */
@Service
public class NotificationTracker {

    public enum Acceptance {
        /** New (or still pending): the caller should publish the event. */
        ACCEPTED,
        /** Already in flight or finished: the caller must not publish again. */
        DUPLICATE
    }

    private final NotificationRecordRepository repository;
    private final ObjectMapper objectMapper;

    public NotificationTracker(NotificationRecordRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    // ---- producer side -----------------------------------------------------------------------

    /** Records the event as PENDING before it is published; deduplicates client retries by event id. */
    public Acceptance accept(NotificationEvent event) {
        Optional<NotificationRecord> existing = repository.findByEventId(event.eventId());
        if (existing.isEmpty()) {
            try {
                repository.saveAndFlush(NotificationRecord.create(event, toJson(event), NotificationStatus.PENDING));
                return Acceptance.ACCEPTED;
            } catch (DataIntegrityViolationException raced) {
                existing = repository.findByEventId(event.eventId());
            }
        }
        // A record that is still PENDING was never picked up (for example the publish failed after the
        // insert), so publishing again is safe; the consumer deduplicates anything that did get through.
        return existing.map(r -> r.getStatus() == NotificationStatus.PENDING ? Acceptance.ACCEPTED : Acceptance.DUPLICATE)
                .orElse(Acceptance.ACCEPTED);
    }

    /** Removes a PENDING record whose event could not be published, so the client can retry cleanly. */
    @Transactional
    public void discardPending(String eventId) {
        repository.findByEventId(eventId)
                .filter(r -> r.getStatus() == NotificationStatus.PENDING)
                .ifPresent(repository::delete);
    }

    // ---- consumer side -----------------------------------------------------------------------

    /**
     * Claims an event for processing.
     *
     * @return {@code false} if the event was already delivered (SENT) or dead-lettered (FAILED) and must
     *     be skipped
     */
    public boolean begin(NotificationEvent event) {
        NotificationRecord record = repository.findByEventId(event.eventId()).orElse(null);
        if (record == null) {
            try {
                repository.saveAndFlush(
                        NotificationRecord.create(event, toJson(event), NotificationStatus.PROCESSING));
                return true;
            } catch (DataIntegrityViolationException raced) {
                record = repository.findByEventId(event.eventId()).orElseThrow(() -> raced);
            }
        }
        if (record.getStatus() == NotificationStatus.SENT || record.getStatus() == NotificationStatus.FAILED) {
            return false;
        }
        record.setStatus(NotificationStatus.PROCESSING);
        repository.save(record);
        return true;
    }

    @Transactional
    public void markSent(String eventId) {
        repository.findByEventId(eventId).ifPresent(r -> {
            r.setStatus(NotificationStatus.SENT);
            r.setLastError(null);
        });
    }

    /** A retryable failure: stays PENDING so a redelivery or backoff retry will pick it up again. */
    @Transactional
    public void markRetrying(String eventId, String error) {
        repository.findByEventId(eventId).ifPresent(r -> {
            r.setStatus(NotificationStatus.PENDING);
            r.setRetryCount(r.getRetryCount() + 1);
            r.setLastError(error);
        });
    }

    @Transactional
    public void markFailed(String eventId, String error) {
        repository.findByEventId(eventId).ifPresent(r -> {
            r.setStatus(NotificationStatus.FAILED);
            r.setLastError(error);
        });
    }

    // ---- support / operations ----------------------------------------------------------------

    @Transactional(readOnly = true)
    public Optional<NotificationRecord> find(String eventId) {
        return repository.findByEventId(eventId);
    }

    @Transactional(readOnly = true)
    public List<NotificationRecord> findByStatus(NotificationStatus status, int limit) {
        return repository.findByStatusOrderByUpdatedAtDesc(status, PageRequest.of(0, Math.max(1, Math.min(limit, 500))));
    }

    /**
     * Resets a FAILED record to PENDING and returns the original event for republishing.
     *
     * @throws NoSuchElementException if the event is unknown
     * @throws IllegalStateException if the event is not FAILED
     */
    @Transactional
    public NotificationEvent prepareReplay(String eventId) {
        NotificationRecord record = repository
                .findByEventId(eventId)
                .orElseThrow(() -> new NoSuchElementException("No notification with event id " + eventId));
        if (record.getStatus() != NotificationStatus.FAILED) {
            throw new IllegalStateException(
                    "Only FAILED notifications can be replayed; " + eventId + " is " + record.getStatus());
        }
        record.setStatus(NotificationStatus.PENDING);
        record.setRetryCount(0);
        return fromJson(record.getPayload());
    }

    // ---- json --------------------------------------------------------------------------------

    private String toJson(NotificationEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise event " + event.eventId(), e);
        }
    }

    private NotificationEvent fromJson(String json) {
        try {
            return objectMapper.readValue(json, NotificationEvent.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored payload cannot be read", e);
        }
    }
}
