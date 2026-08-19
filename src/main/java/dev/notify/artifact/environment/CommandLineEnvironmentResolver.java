package dev.notify.artifact.environment;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Resolves command-line options in {@code --key=value} or {@code --key value} form. */
public final class CommandLineEnvironmentResolver implements EnvironmentResolver {

    private final Map<String, String> options;

    public CommandLineEnvironmentResolver(String... arguments) {
        Objects.requireNonNull(arguments, "arguments");
        this.options = Map.copyOf(parse(arguments));
    }

    @Override
    public boolean containsProperty(String name) {
        return options.containsKey(Objects.requireNonNull(name, "name"));
    }

    @Override
    public String getProperty(String name) {
        return options.get(Objects.requireNonNull(name, "name"));
    }

    private Map<String, String> parse(String[] arguments) {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            String argument = Objects.requireNonNull(arguments[index], "command-line argument");
            if (!argument.startsWith("--") || argument.length() == 2) {
                continue;
            }
            String option = argument.substring(2);
            int separator = option.indexOf('=');
            if (separator >= 0) {
                parsed.put(option.substring(0, separator), option.substring(separator + 1));
            } else if (index + 1 < arguments.length && !arguments[index + 1].startsWith("--")) {
                parsed.put(option, arguments[++index]);
            } else {
                parsed.put(option, "");
            }
        }
        return parsed;
    }
}
