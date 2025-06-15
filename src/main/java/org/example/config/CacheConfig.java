package org.example.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;

import java.util.UUID;

public class CacheConfig {
  public static final String CACHE_NAME = "server-cache";

  public static IgniteCache<UUID, String> createCache(Ignite ignite) {
    CacheConfiguration<UUID, String> config = new CacheConfiguration<>();
    config.setName(CACHE_NAME);
    return ignite.getOrCreateCache(config, new NearCacheConfiguration<>());
  }
}
