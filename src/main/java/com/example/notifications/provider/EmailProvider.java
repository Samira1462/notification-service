package com.example.notifications.provider;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationType;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class EmailProvider extends SimulatedProvider {

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    public EmailProvider(NotificationProperties properties) {
        super(properties);
    }

    @Override
    public NotificationType channel() {
        return NotificationType.EMAIL;
    }

    @Override
    protected void validateRecipient(String recipient) {
        if (recipient == null || !EMAIL.matcher(recipient).matches()) {
            throw new NonRetryableNotificationException("Invalid email address");
        }
    }
}
