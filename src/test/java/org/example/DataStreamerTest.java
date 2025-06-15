package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.junit.jupiter.api.Test;
import org.example.config.ClientNodeConfig;
import org.example.config.ServerNodeConfig;

import java.util.UUID;

import static org.example.config.CacheConfig.createCache;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataStreamerTest {

  @Test
  public void dataStreamerDoNotWorkAfterDeactivate() {
    // запускаем серверную и клиентскую ноду создаем кэш
    Ignite serverNode = ServerNodeConfig.startServerNode();
    Ignite clientNode = ClientNodeConfig.startClient();
    IgniteCache<UUID, String> cache = createCache(clientNode);

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // создаем стример
    IgniteDataStreamer<UUID, String> dataStreamerBeforeDeactivate = clientNode.dataStreamer(cache.getName());

    for (int i = 0; i < 100; i++) {
      dataStreamerBeforeDeactivate.addData(UUID.randomUUID(), "data-" + i);
    }
    assertDoesNotThrow(dataStreamerBeforeDeactivate::flush);

    assertEquals(100, cache.size());

    // деуктивируем кластер и заакрываем стример
    serverNode.cluster().state(ClusterState.INACTIVE);

    assertEquals(ClusterState.INACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.INACTIVE, serverNode.cluster().state());

    dataStreamerBeforeDeactivate.close();

    // активируем кластер
    serverNode.cluster().state(ClusterState.ACTIVE);

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // получаем снова стример, пытаемся загрузить в кэш данные
    IgniteDataStreamer<UUID, String> dataStreamerAfterDeactivate = clientNode.dataStreamer(cache.getName());

    for (int i = 0; i < 100; i++) {
      dataStreamerAfterDeactivate.addData(UUID.randomUUID(), "data-" + i);
    }
    assertThrows(IllegalStateException.class, dataStreamerBeforeDeactivate::flush);

    assertNotEquals(100, cache.size());

    clientNode.close();
    serverNode.close();
  }

  @Test
  public void dataStreamerDoNotWorkAfterInactiveAndRestartServer() {
    // запускаем серверную и клиентскую ноду создаем кэш
    Ignite serverNode = ServerNodeConfig.startServerNode();
    Ignite clientNode = ClientNodeConfig.startClient();
    IgniteCache<UUID, String> cache = createCache(clientNode);

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // создаем стример
    IgniteDataStreamer<UUID, String> dataStreamerBeforeDeactivate = clientNode.dataStreamer(cache.getName());

    for (int i = 0; i < 100; i++) {
      dataStreamerBeforeDeactivate.addData(UUID.randomUUID(), "data-" + i);
    }
    assertDoesNotThrow(dataStreamerBeforeDeactivate::flush);

    assertEquals(100, cache.size());

    // деуктивируем кластер и заакрываем стример
    serverNode.cluster().state(ClusterState.INACTIVE);

    assertEquals(ClusterState.INACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.INACTIVE, serverNode.cluster().state());

    dataStreamerBeforeDeactivate.close();

    // перезапускаем серверную ноду
    serverNode.close();

    serverNode = ServerNodeConfig.startServerNode();
    try {
      // пауза для реконекта клиента
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());

    // пересоздаем кэш
    cache = createCache(clientNode);

    // получаем снова стример, пытаемся загрузить в кэш данные
    IgniteDataStreamer<UUID, String> dataStreamerAfterDeactivate = clientNode.dataStreamer(cache.getName());

    for (int i = 0; i < 100; i++) {
      dataStreamerAfterDeactivate.addData(UUID.randomUUID(), "data-" + i);
    }
    assertThrows(IllegalStateException.class, dataStreamerBeforeDeactivate::flush);

    assertNotEquals(100, cache.size());

    clientNode.close();
    serverNode.close();
  }
}
