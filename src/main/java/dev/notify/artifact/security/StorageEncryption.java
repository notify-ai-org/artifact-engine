package dev.notify.artifact.security;

/** Effective object-store encryption configuration observed by trusted infrastructure. */
public record StorageEncryption(boolean encryptedAtRest, boolean kmsBacked, String keyAlias) {}
