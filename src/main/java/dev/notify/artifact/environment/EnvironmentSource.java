package dev.notify.artifact.environment;

/** A named origin of environment properties. */
public interface EnvironmentSource {

    String getName();

    EnvironmentResolver getResolver();
}
