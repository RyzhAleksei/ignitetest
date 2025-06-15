package org.example.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Collections;
import java.util.UUID;

import static org.example.config.CacheConfig.CACHE_NAME;

public class ClientNodeConfig {
  public static Ignite startClient() {
    return Ignition.start(new IgniteConfiguration()
      .setIgniteInstanceName("client-node")
      .setClientMode(true)
      .setDiscoverySpi(new TcpDiscoverySpi()
        .setNetworkTimeout(10)
        .setIpFinder(new TcpDiscoveryVmIpFinder()
          .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))
        )
      )
    );
  }

  public static IgniteCache<UUID, String> getCache(Ignite ignite) {
    return ignite.cache(CACHE_NAME);
  }
}
