package com.example.notifications.service;

/** The broker did not acknowledge an event in time. The caller may retry with the same event id. */
public class PublishFailedException extends RuntimeException {

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
