package dev.notify.artifact.environment;

import java.util.Map;
import java.util.Objects;

/** Environment source backed by a map. */
public final class MapEnvironmentSource implements EnvironmentSource {

    private final String name;
    private final MapEnvironmentResolver resolver;

    public MapEnvironmentSource(String name, Map<String, ?> values) {
        this.name = Objects.requireNonNull(name, "name");
        this.resolver = new MapEnvironmentResolver(values);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public EnvironmentResolver getResolver() {
        return resolver;
    }
}
