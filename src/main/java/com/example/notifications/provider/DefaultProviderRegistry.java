package com.example.notifications.provider;

import com.example.notifications.model.NotificationType;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DefaultProviderRegistry implements ProviderRegistry {

    private final Map<NotificationType, NotificationProvider> providers = new EnumMap<>(NotificationType.class);

    public DefaultProviderRegistry(List<NotificationProvider> all) {
        for (NotificationProvider provider : all) {
            if (providers.put(provider.channel(), provider) != null) {
                throw new IllegalStateException("More than one provider registered for " + provider.channel());
            }
        }
    }

    @Override
    public NotificationProvider forChannel(NotificationType type) {
        NotificationProvider provider = providers.get(type);
        if (provider == null) {
            throw new NonRetryableNotificationException("No provider configured for channel " + type);
        }
        return provider;
    }
}
