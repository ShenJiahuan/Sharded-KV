package com.shenjiahuan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.shenjiahuan.config.MasterConfig;
import com.shenjiahuan.node.ShardClient;
import com.shenjiahuan.util.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShardServerTest {

  private final Logger logger = Logger.getLogger(getClass());

  public boolean check(ShardClient client, String key, String expectedValue) {
    AtomicReference<String> actualValue = new AtomicReference<>();
    boolean timeout = true;
    for (int i = 0; i < 3; i++) {
      Thread thread = new Thread(() -> actualValue.set(client.get(key)));
      thread.start();
      try {
        thread.join(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (thread.isAlive()) {
        client.abort();
        logger.info("timeout!");
      } else {
        timeout = false;
        break;
      }
    }

    if (timeout) {
      return false;
    }

    logger.info("key: " + key + ", expected: " + expectedValue + ", actual:" + actualValue.get());
    return Objects.equals(expectedValue, actualValue.get());
  }

  @BeforeEach
  private void setUp() throws IOException, InterruptedException {
    final Process p = Runtime.getRuntime().exec("docker exec zoo1 ./reset.sh");
    String s;
    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
    while ((s = br.readLine()) != null) System.out.println(s);
    p.waitFor();
    p.destroy();
  }

  @Test
  public void testGetPutDelete() {
    System.out.println("Test: get, put, delete ...");

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
      client.putOrDelete(key, value, false);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);

      values.set(i, null);
      client.putOrDelete(key, "", true);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    for (int i = 0; i < n; i++) {
      final String key = keys.get(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      values.set(i, value);
      client.putOrDelete(key, value, false);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.cleanUp();

    System.out.println("  ... Passed");
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
      client.putOrDelete(key, value, false);
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

    config.cleanUp();

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
      client.putOrDelete(key, value, false);
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

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    config.leaveGroup(0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = keys.get(i);
      byte[] bytes = new byte[5];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
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

    config.cleanUp();

    System.out.println("  ... Passed");
  }

  @Test
  public void testMultipleJoinLeave() {
    System.out.println("Test: multiple join and leave ...");

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
    final int n = 30;

    final List<String> keys = new ArrayList<>();
    final List<String> values = new ArrayList<>();

    config.joinGroup(0);

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.putOrDelete(key, value, false);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.joinGroup(1);
    config.joinGroup(2);
    config.leaveGroup(0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = keys.get(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    config.leaveGroup(1);
    config.joinGroup(0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = keys.get(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    config.shutDownGroup(0);
    config.shutDownGroup(1);
    config.shutDownGroup(2);

    config.startGroup(0);
    config.startGroup(1);
    config.startGroup(2);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.cleanUp();

    System.out.println("  ... Passed");
  }

  @Test
  public void testMissChange() {
    System.out.println("Test: servers miss configuration changes...");

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
    final int n = 30;

    final List<String> keys = new ArrayList<>();
    final List<String> values = new ArrayList<>();

    config.joinGroup(0);

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.putOrDelete(key, value, false);
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.joinGroup(1);

    config.shutDownServer(0, 0);
    config.shutDownServer(1, 0);
    config.shutDownServer(2, 0);

    config.joinGroup(2);
    config.leaveGroup(1);
    config.leaveGroup(0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    config.joinGroup(1);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    config.startServer(0, 0);
    config.startServer(1, 0);
    config.startServer(2, 0);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    config.shutDownServer(0, 1);
    config.shutDownServer(1, 1);
    config.shutDownServer(2, 1);

    config.joinGroup(0);
    config.leaveGroup(2);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));

      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      client.putOrDelete(key, value, false);
      values.set(i, value);
    }

    config.startServer(0, 1);
    config.startServer(1, 1);
    config.startServer(2, 1);

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.cleanUp();

    System.out.println("  ... Passed");
  }

  @Test
  public void testConcurrent1() {
    System.out.println("Test: concurrent puts and configuration changes...");

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
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.putOrDelete(key, value, false);
    }

    final AtomicBoolean done = new AtomicBoolean(false);
    final List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      final int index = i;
      final Thread t =
          new Thread(
              () -> {
                final ShardClient client1 = config.createShardClient();
                while (!done.get()) {
                  assertTrue(check(client, keys.get(index), values.get(index)));
                  final String key = String.valueOf(index);
                  byte[] bytes = new byte[20];
                  new Random().nextBytes(bytes);
                  final String value = new String(bytes);

                  values.set(index, value);
                  client1.putOrDelete(key, value, false);
                  Utils.sleep(ThreadLocalRandom.current().nextLong(10, 100));
                }
              });
      t.start();
      threads.add(t);
    }

    Utils.sleep(150);
    config.joinGroup(1);
    Utils.sleep(500);
    config.joinGroup(2);
    Utils.sleep(500);
    config.leaveGroup(0);

    config.shutDownGroup(0);
    Utils.sleep(100);
    config.shutDownGroup(1);
    Utils.sleep(100);
    config.shutDownGroup(2);

    config.leaveGroup(2);

    Utils.sleep(100);
    config.startGroup(0);
    config.startGroup(1);
    config.startGroup(2);

    Utils.sleep(100);
    config.joinGroup(0);
    config.leaveGroup(1);
    Utils.sleep(500);
    config.joinGroup(1);

    Utils.sleep(1000);

    done.set(true);

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.cleanUp();

    System.out.println("  ... Passed");
  }

  @Test
  public void testConcurrent2() {
    System.out.println("Test: more concurrent puts and configuration changes...");

    System.out.println("Test: concurrent puts and configuration changes...");

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

    config.joinGroup(1);
    config.joinGroup(0);
    config.joinGroup(2);

    for (int i = 0; i < n; i++) {
      final String key = String.valueOf(i);
      byte[] bytes = new byte[20];
      new Random().nextBytes(bytes);
      final String value = new String(bytes);

      keys.add(key);
      values.add(value);
      client.putOrDelete(key, value, false);
    }

    final AtomicBoolean done = new AtomicBoolean(false);
    final List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < n; i++) {
      final int index = i;
      final Thread t =
          new Thread(
              () -> {
                final ShardClient client1 = config.createShardClient();
                while (!done.get()) {
                  assertTrue(check(client, keys.get(index), values.get(index)));
                  final String key = String.valueOf(index);
                  byte[] bytes = new byte[20];
                  new Random().nextBytes(bytes);
                  final String value = new String(bytes);

                  values.set(index, value);
                  client1.putOrDelete(key, value, false);
                  Utils.sleep(ThreadLocalRandom.current().nextLong(10, 100));
                }
              });
      t.start();
      threads.add(t);
    }

    config.leaveGroup(0);
    config.leaveGroup(2);
    Utils.sleep(3000);
    config.joinGroup(0);
    config.joinGroup(2);
    config.leaveGroup(1);
    Utils.sleep(3000);
    config.joinGroup(1);
    config.leaveGroup(0);
    config.leaveGroup(2);
    Utils.sleep(3000);

    config.shutDownGroup(1);
    config.shutDownGroup(2);
    Utils.sleep(1000);
    config.startGroup(1);
    config.startGroup(2);

    Utils.sleep(2000);

    done.set(true);

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < n; i++) {
      assertTrue(check(client, keys.get(i), values.get(i)));
    }

    config.cleanUp();

    System.out.println("  ... Passed");
  }
}
