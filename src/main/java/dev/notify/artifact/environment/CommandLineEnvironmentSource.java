package dev.notify.artifact.environment;

/** Source for command-line options. */
public final class CommandLineEnvironmentSource implements EnvironmentSource {

    public static final String NAME = "commandLineArgs";

    private final CommandLineEnvironmentResolver resolver;

    public CommandLineEnvironmentSource(String... arguments) {
        this.resolver = new CommandLineEnvironmentResolver(arguments);
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
