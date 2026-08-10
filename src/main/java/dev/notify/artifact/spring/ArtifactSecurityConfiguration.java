package dev.notify.artifact.spring;

import dev.notify.artifact.auth.DataVerifier;
import dev.notify.artifact.security.ArtifactSecurity;
import dev.notify.artifact.security.AuthenticationService;
import dev.notify.artifact.security.IngestionSecurityFilters;
import dev.notify.artifact.security.MalwareScanner;
import dev.notify.artifact.security.RetrievalSecurityFilters;
import dev.notify.artifact.security.SecurityContextFactory;
import dev.notify.artifact.security.UrlIngestionPolicyFilter;
import java.time.Clock;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Activates the complete filter layer only when the application supplies all security adapters. */
@Configuration(proxyBeanMethods = false)
public class ArtifactSecurityConfiguration {
  @Bean
  @ConditionalOnMissingBean
  public UrlIngestionPolicyFilter artifactUrlIngestionPolicyFilter() {
    String configuredHosts = System.getProperty("artifact.security.url.allowed-hosts", "");
    Set<String> allowedHosts =
        Arrays.stream(configuredHosts.split(","))
            .map(String::trim)
            .filter(value -> !value.isBlank())
            .collect(Collectors.toUnmodifiableSet());
    return new UrlIngestionPolicyFilter(
        Set.of("https"), allowedHosts, UrlIngestionPolicyFilter.HostResolver.system());
  }

  @Bean
  @ConditionalOnBean({
    AuthenticationService.class,
    MalwareScanner.class,
    SecurityContextFactory.class
  })
  @ConditionalOnMissingBean
  public ArtifactSecurity artifactSecurity(
      AuthenticationService authentication,
      MalwareScanner malwareScanner,
      SecurityContextFactory contextFactory,
      DataVerifier dataVerifier,
      UrlIngestionPolicyFilter urlPolicy) {
    return new ArtifactSecurity(
        IngestionSecurityFilters.create(
            authentication,
            dataVerifier,
            malwareScanner,
            urlPolicy,
            IngestionSecurityFilters.Policy.defaults(),
            Clock.systemUTC()),
        RetrievalSecurityFilters.create(
            authentication, RetrievalSecurityFilters.Policy.defaults(), Clock.systemUTC()),
        contextFactory);
  }
}
