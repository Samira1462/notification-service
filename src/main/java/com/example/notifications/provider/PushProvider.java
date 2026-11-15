package com.example.notifications.provider;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationType;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class PushProvider extends SimulatedProvider {

    private static final Pattern DEVICE_TOKEN = Pattern.compile("^[A-Za-z0-9:_\\-]{8,}$");

    public PushProvider(NotificationProperties properties) {
        super(properties);
    }

    @Override
    public NotificationType channel() {
        return NotificationType.PUSH;
    }

    @Override
    protected void validateRecipient(String recipient) {
        if (recipient == null || !DEVICE_TOKEN.matcher(recipient).matches()) {
            throw new NonRetryableNotificationException("Invalid device token");
        }
    }
}
