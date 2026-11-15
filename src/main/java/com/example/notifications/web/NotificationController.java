package com.example.notifications.web;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationEvent;
import com.example.notifications.model.NotificationStatus;
import com.example.notifications.service.NotificationIngestService;
import com.example.notifications.service.NotificationTracker;
import jakarta.validation.Valid;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP facade standing in for the "business services" of the design (orders, payments, auth) plus the
 * support/ops endpoints. Real services would call {@code NotificationPublisher} directly, or use this API.
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationIngestService ingest;
    private final NotificationTracker tracker;
    private final NotificationProperties properties;

    public NotificationController(
            NotificationIngestService ingest, NotificationTracker tracker, NotificationProperties properties) {
        this.ingest = ingest;
        this.tracker = tracker;
        this.properties = properties;
    }

    /** 202 when published, 200 with {@code duplicate=true} when this event id was already accepted. */
    @PostMapping
    public ResponseEntity<AcceptedResponse> send(@Valid @RequestBody SendNotificationRequest request) {
        String eventId = request.eventId() == null || request.eventId().isBlank()
                ? UUID.randomUUID().toString()
                : request.eventId();
        NotificationEvent event = new NotificationEvent(
                eventId,
                request.userId(),
                request.type(),
                request.recipient(),
                request.templateId(),
                request.data(),
                Instant.now());

        boolean published = ingest.submit(event);
        AcceptedResponse body = new AcceptedResponse(eventId, properties.topicFor(event.type()), !published);
        return ResponseEntity.status(published ? HttpStatus.ACCEPTED : HttpStatus.OK).body(body);
    }

    @GetMapping("/{eventId}")
    public NotificationView get(@PathVariable String eventId) {
        return tracker.find(eventId)
                .map(NotificationView::from)
                .orElseThrow(() -> new NoSuchElementException("No notification with event id " + eventId));
    }

    /** e.g. {@code GET /api/notifications?status=FAILED} to inspect dead-lettered notifications. */
    @GetMapping
    public List<NotificationView> list(
            @RequestParam NotificationStatus status, @RequestParam(defaultValue = "50") int limit) {
        return tracker.findByStatus(status, limit).stream().map(NotificationView::from).toList();
    }

    /** Republishes a FAILED notification once its cause (provider outage, bad template) is fixed. */
    @PostMapping("/{eventId}/replay")
    public ResponseEntity<AcceptedResponse> replay(@PathVariable String eventId) {
        NotificationEvent event = ingest.replay(eventId);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new AcceptedResponse(event.eventId(), properties.topicFor(event.type()), false));
    }
}
