package com.example.notifications.service;

import com.example.notifications.provider.NonRetryableNotificationException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * Renders {@code {{placeholder}}} templates. Events carry only a template id plus data, which keeps
 * Kafka messages small. Templates live in code here; move them to a database or config service when
 * non-developers need to edit them.
 */
@Component
public class TemplateRenderer {

    public record Rendered(String subject, String body) {}

    private record Template(String subject, String body) {}

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*(\\w+)\\s*\\}\\}");

    private static final Map<String, Template> TEMPLATES = Map.of(
            "ORDER_CONFIRMATION",
            new Template(
                    "Order {{orderId}} confirmed",
                    "Hi {{name}}, your order {{orderId}} totalling {{total}} is confirmed."),
            "PAYMENT_RECEIVED",
            new Template(
                    "Payment received",
                    "Hi {{name}}, we received your payment of {{amount}}. Thank you!"),
            "SHIPPING_UPDATE",
            new Template(
                    "Your order {{orderId}} has shipped",
                    "Hi {{name}}, order {{orderId}} is on its way. Tracking: {{trackingNumber}}."),
            "PASSWORD_RESET",
            new Template(
                    "Reset your password",
                    "Hi {{name}}, use this code to reset your password: {{code}}. It expires in 15 minutes."));

    /** @throws NonRetryableNotificationException for an unknown template or a missing variable */
    public Rendered render(String templateId, Map<String, String> data) {
        Template template = templateId == null ? null : TEMPLATES.get(templateId);
        if (template == null) {
            throw new NonRetryableNotificationException("Unknown template: " + templateId);
        }
        return new Rendered(fill(template.subject(), data), fill(template.body(), data));
    }

    private static String fill(String text, Map<String, String> data) {
        Matcher m = PLACEHOLDER.matcher(text);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String value = data.get(m.group(1));
            if (value == null) {
                throw new NonRetryableNotificationException("Missing template variable: " + m.group(1));
            }
            m.appendReplacement(out, Matcher.quoteReplacement(value));
        }
        m.appendTail(out);
        return out.toString();
    }
}
