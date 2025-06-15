package org.example.config;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;

public class ServerNodeConfig {

  public static Ignite startServerNode() {
    return Ignition.start(new IgniteConfiguration()
      .setIgniteInstanceName("server-node")
    );
  }
}
