package com.example.notifications.service;

import com.example.notifications.config.NotificationProperties;
import com.example.notifications.config.NotificationProperties.RateLimit;
import com.example.notifications.model.NotificationType;
import com.example.notifications.provider.RetryableNotificationException;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Per-channel outbound throttle. Scaling out workers can easily exceed a provider's contractual rate,
 * so every send first needs a permit. A caller waits up to the configured timeout for a permit; if none
 * arrives the failure is retryable and the normal backoff takes over.
 *
 * <p>The limit is per application instance. For a cluster-wide limit, divide the contract limit by the
 * instance count or back the limiter with a shared store such as Redis.
 */
@Component
public class ChannelRateLimiters {

    private static final RateLimit DEFAULT_LIMIT = new RateLimit(100, Duration.ofSeconds(1), Duration.ofSeconds(5));

    private final Map<NotificationType, RateLimiter> limiters = new EnumMap<>(NotificationType.class);

    public ChannelRateLimiters(NotificationProperties properties) {
        Map<NotificationType, RateLimit> configured = properties.rateLimit();
        for (NotificationType type : NotificationType.values()) {
            RateLimit limit = configured == null ? DEFAULT_LIMIT : configured.getOrDefault(type, DEFAULT_LIMIT);
            RateLimiterConfig config = RateLimiterConfig.custom()
                    .limitForPeriod(limit.limitForPeriod())
                    .limitRefreshPeriod(limit.refreshPeriod())
                    .timeoutDuration(limit.timeout())
                    .build();
            limiters.put(type, RateLimiter.of("notifications-" + type.name().toLowerCase(), config));
        }
    }

    public void acquire(NotificationType type) {
        if (!limiters.get(type).acquirePermission()) {
            throw new RetryableNotificationException("Outbound rate limit reached for " + type);
        }
    }
}
