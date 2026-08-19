package dev.notify.artifact.environment;

import java.util.List;

/** An ordered collection of environment sources and a resolver over them. */
public interface Environment extends EnvironmentResolver {

    List<EnvironmentSource> getSources();
}
