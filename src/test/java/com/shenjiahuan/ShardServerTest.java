package com.shenjiahuan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.shenjiahuan.config.MasterConfig;
import com.shenjiahuan.node.ShardClient;
import com.shenjiahuan.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

public class ShardServerTest {

  private final Logger logger = Logger.getLogger(getClass());

  public boolean check(ShardClient client, String key, String expectedValue) {
    AtomicReference<String> actualValue = new AtomicReference<>();
    Thread thread = new Thread(() -> actualValue.set(client.get(key)));
    thread.start();
    try {
      thread.join(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (thread.isAlive()) {
      client.abort();
      return false;
    }

    logger.info("key: " + key + ", expected: " + expectedValue + ", actual:" + actualValue.get());
    return expectedValue.equals(actualValue.get());
  }

  @Test
  public void testStaticShards() {
    System.out.println("Test: static shards ...");

    final TestShardServerConfig config =
        new TestShardServerConfig(
            3,
            Arrays.asList(1234, 1235, 1236),
            3,
            3,
            Arrays.asList(
                Arrays.asList(20001, 20002, 20003),
                Arrays.asList(30001, 30002, 30003),
                Arrays.asList(40001, 40002, 40003)));

    final ShardClient client = config.createShardClient();
    client.init();
    final int n = 10;

    config.joinGroup(0);
    config.joinGroup(1);

    final List<String> keys = new ArrayList<>();
    final List<String> values = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.put(key, value);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.shutDownGroup(1);

    AtomicInteger nPass = new AtomicInteger(0);
    AtomicInteger nDone = new AtomicInteger(0);
    for (int i = 0; i < n; i++) {
      final ShardClient client1 = config.createShardClient();
      final String key = keys.get(i);
      final String value = values.get(i);
      new Thread(
              () -> {
                if (check(client1, key, value)) {
                  nPass.incrementAndGet();
                }
                nDone.incrementAndGet();
                logger.info("nPass:" + nPass.get() + ", nDone: " + nDone.get());
              })
          .start();
    }

    while (nDone.get() != n) {
      Thread.yield();
    }

    assertEquals(
        IntStream.range(0, n)
            .filter(x -> Utils.key2Shard(String.valueOf(x)) < MasterConfig.SHARD_COUNT / 2)
            .count(),
        nPass.get());

    config.startGroup(1);
    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    System.out.println("  ... Passed");
  }

  @Test
  public void testJoinLeave() {
    System.out.println("Test: join then leave ...");

    final TestShardServerConfig config =
        new TestShardServerConfig(
            3,
            Arrays.asList(1234, 1235, 1236),
            3,
            3,
            Arrays.asList(
                Arrays.asList(20001, 20002, 20003),
                Arrays.asList(30001, 30002, 30003),
                Arrays.asList(40001, 40002, 40003)));

    final ShardClient client = config.createShardClient();
    client.init();
    final int n = 10;

    final List<String> keys = new ArrayList<>();
    final List<String> values = new ArrayList<>();

    config.joinGroup(0);

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);
      byte[] bytes = new byte[5];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.put(key, value);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.joinGroup(1);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = keys.get(i);
      byte[] bytes = new byte[5];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.put(key, value);
      values.set(i, value);
    }

    config.leaveGroup(0);
    //    try {
    //      Thread.sleep(1000000);
    //    } catch (InterruptedException e) {
    //      e.printStackTrace();
    //    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = keys.get(i);
      byte[] bytes = new byte[5];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.put(key, value);
      values.set(i, value);
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    config.shutDownGroup(0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    System.out.println("  ... Passed");
  }
}
