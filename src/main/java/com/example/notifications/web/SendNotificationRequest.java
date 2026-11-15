package com.example.notifications.web;

import com.example.notifications.model.NotificationType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Map;

public record SendNotificationRequest(
        /** Optional client-supplied idempotency key. Reuse it when retrying a request. */
        @Size(max = 100) String eventId,
        @NotBlank @Size(max = 255) String userId,
        @NotNull NotificationType type,
        @NotBlank @Size(max = 255) String recipient,
        @NotBlank @Size(max = 100) String templateId,
        @Size(max = 50) Map<String, String> data) {}
