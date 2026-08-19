package dev.notify.artifact.environment;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves values from an immutable map snapshot. */
public final class MapEnvironmentResolver implements EnvironmentResolver {

    private final Map<String, String> values;

    public MapEnvironmentResolver(Map<String, ?> values) {
        Objects.requireNonNull(values, "values");
        Map<String, String> copy = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            Objects.requireNonNull(key, "environment property name");
            if (value != null) {
                copy.put(key, value.toString());
            }
        });
        this.values = Map.copyOf(copy);
    }

    @Override
    public boolean containsProperty(String name) {
        return values.containsKey(Objects.requireNonNull(name, "name"));
    }

    @Override
    public String getProperty(String name) {
        return values.get(Objects.requireNonNull(name, "name"));
    }
}
