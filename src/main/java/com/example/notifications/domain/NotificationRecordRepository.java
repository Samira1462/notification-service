package com.example.notifications.domain;

import com.example.notifications.model.NotificationStatus;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationRecordRepository extends JpaRepository<NotificationRecord, Long> {

    Optional<NotificationRecord> findByEventId(String eventId);

    List<NotificationRecord> findByStatusOrderByUpdatedAtDesc(NotificationStatus status, Pageable pageable);

    long countByStatus(NotificationStatus status);
}
