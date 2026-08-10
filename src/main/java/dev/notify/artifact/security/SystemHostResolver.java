package dev.notify.artifact.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/** Default host resolver backed by the JVM and operating-system DNS configuration. */
public final class SystemHostResolver implements UrlIngestionPolicyFilter.HostResolver {
  public static final SystemHostResolver INSTANCE = new SystemHostResolver();

  private SystemHostResolver() {}

  @Override
  public List<InetAddress> resolve(String host) throws UnknownHostException {
    return Arrays.asList(InetAddress.getAllByName(host));
  }
}
