package dev.notify.artifact.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

class StandardEnvironmentTest {

  @Test
  void resolvesFirstMatchingSource() {
    Environment environment =
        new StandardEnvironment(
            new MapEnvironmentSource("override", Map.of("app.name", "command-line")),
            new MapEnvironmentSource(
                "defaults", Map.of("app.name", "default", "app.port", 8080)));

    assertEquals("command-line", environment.getProperty("app.name"));
    assertEquals("8080", environment.getProperty("app.port"));
    assertEquals("fallback", environment.getProperty("missing", "fallback"));
    assertThrows(NoSuchElementException.class, () -> environment.getRequiredProperty("missing"));
  }

  @Test
  void resolvesSystemEnvironmentUsingRelaxedNames() {
    EnvironmentResolver resolver =
        new SystemEnvironmentResolver(Map.of("SERVER_PORT", "9090", "JAVA_HOME", "/java"));

    assertEquals("9090", resolver.getProperty("server.port"));
    assertEquals("/java", resolver.getProperty("java-home"));
    assertFalse(resolver.containsProperty("missing"));
  }

  @Test
  void loadsProperties() throws Exception {
    byte[] content =
        "service.url=https://example.test\nretries=3\n"
            .getBytes(StandardCharsets.ISO_8859_1);
    EnvironmentSource source =
        new PropertiesFileEnvironmentSource(
            "test.properties", new ByteArrayInputStream(content));

    assertEquals("https://example.test", source.getResolver().getProperty("service.url"));
    assertEquals("3", source.getResolver().getProperty("retries"));
  }

  @Test
  void parsesCommandLineOptions() {
    EnvironmentResolver resolver =
        new CommandLineEnvironmentResolver(
            "input.txt", "--server.port=8081", "--profile", "local", "--debug");

    assertEquals("8081", resolver.getProperty("server.port"));
    assertEquals("local", resolver.getProperty("profile"));
    assertEquals("", resolver.getProperty("debug"));
    assertTrue(resolver.containsProperty("debug"));
  }
}
