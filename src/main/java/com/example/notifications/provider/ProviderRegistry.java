package com.example.notifications.provider;

import com.example.notifications.model.NotificationType;

/** Looks up the provider for a channel. An interface so tests can swap in a scripted provider. */
public interface ProviderRegistry {

    NotificationProvider forChannel(NotificationType type);
}
