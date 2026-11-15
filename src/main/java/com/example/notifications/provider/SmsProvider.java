package com.example.notifications.provider;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.model.NotificationType;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

@Component
public class SmsProvider extends SimulatedProvider {

    /** E.164: a plus sign followed by 7 to 15 digits, not starting with 0. */
    private static final Pattern E164 = Pattern.compile("^\\+[1-9]\\d{6,14}$");

    public SmsProvider(NotificationProperties properties) {
        super(properties);
    }

    @Override
    public NotificationType channel() {
        return NotificationType.SMS;
    }

    @Override
    protected void validateRecipient(String recipient) {
        if (recipient == null || !E164.matcher(recipient).matches()) {
            throw new NonRetryableNotificationException("Invalid phone number (expected E.164)");
        }
    }
}
