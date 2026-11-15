package com.example.notifications.web;

public record AcceptedResponse(String eventId, String topic, boolean duplicate) {}
