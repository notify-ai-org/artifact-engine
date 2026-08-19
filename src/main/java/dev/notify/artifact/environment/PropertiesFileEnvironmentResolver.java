package dev.notify.artifact.environment;

import java.util.Properties;

/** Resolves values loaded from a Java properties file. */
public final class PropertiesFileEnvironmentResolver implements EnvironmentResolver {

    private final Properties properties;

    public PropertiesFileEnvironmentResolver(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    @Override
    public boolean containsProperty(String name) {
        return properties.containsKey(name);
    }

    @Override
    public String getProperty(String name) {
        return properties.getProperty(name);
    }
}
