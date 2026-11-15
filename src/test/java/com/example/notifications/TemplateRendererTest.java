package com.example.notifications;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.notifications.provider.NonRetryableNotificationException;
import com.example.notifications.service.TemplateRenderer;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TemplateRendererTest {

    private final TemplateRenderer renderer = new TemplateRenderer();

    @Test
    void fillsPlaceholdersInSubjectAndBody() {
        TemplateRenderer.Rendered r = renderer.render(
                "ORDER_CONFIRMATION", Map.of("name", "Ada", "orderId", "A-17", "total", "$42.00"));

        assertThat(r.subject()).isEqualTo("Order A-17 confirmed");
        assertThat(r.body()).isEqualTo("Hi Ada, your order A-17 totalling $42.00 is confirmed.");
    }

    @Test
    void unknownTemplateIsNonRetryable() {
        assertThatThrownBy(() -> renderer.render("NOPE", Map.of()))
                .isInstanceOf(NonRetryableNotificationException.class)
                .hasMessageContaining("Unknown template");
    }

    @Test
    void missingVariableIsNonRetryable() {
        assertThatThrownBy(() -> renderer.render("PAYMENT_RECEIVED", Map.of("name", "Ada")))
                .isInstanceOf(NonRetryableNotificationException.class)
                .hasMessageContaining("amount");
    }
}
