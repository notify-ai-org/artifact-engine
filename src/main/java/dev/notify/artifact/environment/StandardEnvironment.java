package dev.notify.artifact.environment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Default environment implementation. Sources are queried in declaration order. */
public final class StandardEnvironment implements Environment {

    private final List<EnvironmentSource> sources;

    public StandardEnvironment(EnvironmentSource... sources) {
        this(Arrays.asList(sources));
    }

    public StandardEnvironment(List<? extends EnvironmentSource> sources) {
        Objects.requireNonNull(sources, "sources");
        List<EnvironmentSource> copy = new ArrayList<>(sources.size());
        for (EnvironmentSource source : sources) {
            copy.add(Objects.requireNonNull(source, "source"));
        }
        this.sources = List.copyOf(copy);
    }

    @Override
    public List<EnvironmentSource> getSources() {
        return sources;
    }

    @Override
    public boolean containsProperty(String name) {
        Objects.requireNonNull(name, "name");
        return sources.stream().anyMatch(source -> source.getResolver().containsProperty(name));
    }

    @Override
    public String getProperty(String name) {
        Objects.requireNonNull(name, "name");
        for (EnvironmentSource source : sources) {
            String value = source.getResolver().getProperty(name);
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
