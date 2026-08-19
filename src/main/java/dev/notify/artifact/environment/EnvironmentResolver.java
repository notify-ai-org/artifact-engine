package dev.notify.artifact.environment;

import java.util.NoSuchElementException;

/** Resolves configuration values by property name. */
public interface EnvironmentResolver {

    boolean containsProperty(String name);

    String getProperty(String name);

    default String getProperty(String name, String defaultValue) {
        String value = getProperty(name);
        return value != null ? value : defaultValue;
    }

    default String getRequiredProperty(String name) {
        String value = getProperty(name);
        if (value == null) {
            throw new NoSuchElementException("Required environment property '" + name + "' was not found");
        }
        return value;
    }
}
