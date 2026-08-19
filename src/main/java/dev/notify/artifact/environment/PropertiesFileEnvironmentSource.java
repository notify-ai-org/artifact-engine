package dev.notify.artifact.environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/** Source loaded eagerly from a standard Java {@code .properties} file. */
public final class PropertiesFileEnvironmentSource implements EnvironmentSource {

    private final String name;
    private final PropertiesFileEnvironmentResolver resolver;

    public PropertiesFileEnvironmentSource(Path path) throws IOException {
        this(path.toString(), path);
    }

    public PropertiesFileEnvironmentSource(String name, Path path) throws IOException {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(path, "path");
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
        }
        this.resolver = new PropertiesFileEnvironmentResolver(properties);
    }

    public PropertiesFileEnvironmentSource(String name, InputStream input) throws IOException {
        this.name = Objects.requireNonNull(name, "name");
        Objects.requireNonNull(input, "input");
        Properties properties = new Properties();
        properties.load(input);
        this.resolver = new PropertiesFileEnvironmentResolver(properties);
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
