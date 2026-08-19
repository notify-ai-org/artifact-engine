package dev.notify.artifact.environment;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Resolves OS variables, including relaxed forms such as {@code server.port -> SERVER_PORT}. */
public final class SystemEnvironmentResolver implements EnvironmentResolver {

    private final Map<String, String> variables;

    public SystemEnvironmentResolver(Map<String, String> variables) {
        this.variables = Map.copyOf(Objects.requireNonNull(variables, "variables"));
    }

    @Override
    public boolean containsProperty(String name) {
        return resolveName(name) != null;
    }

    @Override
    public String getProperty(String name) {
        String resolvedName = resolveName(name);
        return resolvedName == null ? null : variables.get(resolvedName);
    }

    private String resolveName(String name) {
        Objects.requireNonNull(name, "name");
        for (String candidate : candidates(name)) {
            if (variables.containsKey(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private Set<String> candidates(String name) {
        Set<String> candidates = new LinkedHashSet<>();
        candidates.add(name);
        candidates.add(name.replace('.', '_'));
        candidates.add(name.replace('-', '_'));
        candidates.add(name.replace('.', '_').replace('-', '_'));
        for (String candidate : Set.copyOf(candidates)) {
            candidates.add(candidate.toUpperCase());
        }
        return candidates;
    }
}
