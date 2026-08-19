package dev.notify.artifact.environment;

import java.util.Map;

/** Source for process environment variables. */
public final class SystemEnvironmentSource implements EnvironmentSource {

    public static final String NAME = "systemEnvironment";

    private final SystemEnvironmentResolver resolver;

    public SystemEnvironmentSource() {
        this(System.getenv());
    }

    public SystemEnvironmentSource(Map<String, String> variables) {
        this.resolver = new SystemEnvironmentResolver(variables);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public EnvironmentResolver getResolver() {
        return resolver;
    }
}
