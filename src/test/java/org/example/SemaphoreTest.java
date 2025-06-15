package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cluster.ClusterState;
import org.junit.jupiter.api.Test;
import org.example.config.ClientNodeConfig;
import org.example.config.ServerNodeConfig;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class SemaphoreTest {

  @Test
  public void semaphoreDoNotAcquireAfterInactive() {
    // запускаем серверную и клиентскую ноду
    Ignite serverNode = ServerNodeConfig.startServerNode();
    Ignite clientNode = ClientNodeConfig.startClient();

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // создаем семафор и получаем пермит
    IgniteSemaphore semaphoreBeforeDeactivation = clientNode.semaphore("semaphore", 1, true, true);

    assertFalse(semaphoreBeforeDeactivation.isBroken());
    boolean acquire = semaphoreBeforeDeactivation.tryAcquire();

    assertTrue(acquire);
    assertEquals(0, semaphoreBeforeDeactivation.availablePermits());

    // деуктивируем кластер не отпуская семафор
    serverNode.cluster().state(ClusterState.INACTIVE);

    assertEquals(ClusterState.INACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.INACTIVE, serverNode.cluster().state());

    // активируем кластер
    serverNode.cluster().state(ClusterState.ACTIVE);

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // удаляем семафор (ожидаемое поведение)
    assertDoesNotThrow(semaphoreBeforeDeactivation::close);

    // создаем новый семафор (ожидаемое поведение)
    IgniteSemaphore semaphoreAfterDeactivation = clientNode.semaphore("semaphore", 1, true, true);

    // убеждаемся что получили тот же семафор и он не рабочий
    assertTrue(semaphoreAfterDeactivation.isBroken());
    assertThrows(IgniteInterruptedException.class, semaphoreAfterDeactivation::acquire);
    assertTrue(semaphoreAfterDeactivation == semaphoreBeforeDeactivation);

    clientNode.close();
    serverNode.close();
  }

  @Test
  public void semaphoreAcquireAfterInactiveAndRestartClient() {
    // запускаем серверную и клиентскую ноду
    Ignite serverNode = ServerNodeConfig.startServerNode();
    Ignite clientNode = ClientNodeConfig.startClient();

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // создаем семафор и получаем пермит
    IgniteSemaphore semaphore = clientNode.semaphore("semaphore", 1, true, true);

    assertFalse(semaphore.isBroken());
    boolean acquire = semaphore.tryAcquire();

    assertTrue(acquire);
    assertEquals(0, semaphore.availablePermits());

    // деуктивируем кластер не отпуская семафор
    serverNode.cluster().state(ClusterState.INACTIVE);

    assertEquals(ClusterState.INACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.INACTIVE, serverNode.cluster().state());

    // активируем кластер
    serverNode.cluster().state(ClusterState.ACTIVE);

    assertEquals(ClusterState.ACTIVE, clientNode.cluster().state());
    assertEquals(ClusterState.ACTIVE, serverNode.cluster().state());

    // перезапускаем клиента
    clientNode.close();
    clientNode = ClientNodeConfig.startClient();

    // создаем новый семафор
    semaphore = clientNode.semaphore("semaphore", 1, true, true);

    // убеждаемся что семафор рабочий
    AtomicReference<IgniteSemaphore> semaphoreRef = new AtomicReference<>();
    semaphoreRef.set(semaphore);

    assertFalse(semaphore.isBroken());
    assertDoesNotThrow(() -> semaphoreRef.get().tryAcquire());

    clientNode.close();
    serverNode.close();
  }
}
