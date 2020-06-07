package com.shenjiahuan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.shenjiahuan.config.MasterConfig;
import com.shenjiahuan.node.MasterClient;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

public class MasterTest {

  private void check(MasterClient masterClient, Set<Long> groups) {
    final String queryResponse = masterClient.query(-1);

    final Map<Long, List<Server>> groupMap = getGroupMap(queryResponse);

    assertEquals(groups, groupMap.keySet());

    if (groups.size() > 0) {
      final Map<Long, List<Long>> shardMap = getShardMap(queryResponse);

      final List<Long> shards =
          shardMap
              .values()
              .stream()
              .flatMap(Collection::stream)
              .sorted()
              .collect(Collectors.toList());

      final List<Long> expectedShards =
          LongStream.range(0, MasterConfig.SHARD_COUNT.intValue())
              .boxed()
              .collect(Collectors.toList());

      assertEquals(expectedShards, shards);
    }
  }

  private Map<Long, List<Server>> getGroupMap(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return new Gson()
        .fromJson(
            jsonObject.get("groupMap").toString(),
            new TypeToken<Map<Long, List<Server>>>() {}.getType());
  }

  private Map<Long, List<Long>> getShardMap(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return new Gson()
        .fromJson(
            jsonObject.get("shardMap").toString(),
            new TypeToken<Map<Long, List<Long>>>() {}.getType());
  }

  private long getContainingGroup(String queryResponse, long shardId) {
    return getShardMap(queryResponse)
        .entrySet()
        .stream()
        .filter(e -> e.getValue().contains(shardId))
        .collect(Collectors.toList())
        .get(0)
        .getKey();
  }

  private int getVersion(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return jsonObject.get("version").getAsInt();
  }

  @Test
  public void testBasic() {
    Config config = new Config(3, Arrays.asList(1234, 1235, 1236));

    String[] queryResult = new String[6];
    MasterClient client = config.createClient();
    final int nParallelClient = 10;

    System.out.println("Test: Basic leave/join ...");
    {
      check(client, new HashSet<>(Collections.emptyList()));
      queryResult[0] = client.query(-1);

      final long gid1 = 1L;
      client.join(
          gid1,
          Arrays.asList(
              new Pair<>("localhost", 10000),
              new Pair<>("localhost", 10001),
              new Pair<>("localhost", 10002)));
      check(client, new HashSet<>(Collections.singletonList(gid1)));
      queryResult[1] = client.query(-1);

      final long gid2 = 2L;
      client.join(
          gid2,
          Arrays.asList(
              new Pair<>("localhost", 10003),
              new Pair<>("localhost", 10004),
              new Pair<>("localhost", 10005)));
      check(client, new HashSet<>(Arrays.asList(gid1, gid2)));
      queryResult[2] = client.query(-1);

      client.join(
          gid2,
          Arrays.asList(
              new Pair<>("localhost", 10003),
              new Pair<>("localhost", 10004),
              new Pair<>("localhost", 10005)));
      check(client, new HashSet<>(Arrays.asList(gid1, gid2)));
      queryResult[3] = client.query(-1);

      final String res1 = client.query(-1);
      final Map<Long, List<Server>> groupMap1 = getGroupMap(res1);

      List<Server> serverList1 = groupMap1.get(gid1);
      assertEquals(3, serverList1.size());
      assertTrue(
          serverList1.get(0).getHost().equals("localhost")
              && serverList1.get(0).getPort() == 10000
              && serverList1.get(1).getHost().equals("localhost")
              && serverList1.get(1).getPort() == 10001
              && serverList1.get(2).getHost().equals("localhost")
              && serverList1.get(2).getPort() == 10002);

      List<Server> serverList2 = groupMap1.get(gid2);
      assertEquals(3, serverList2.size());
      assertTrue(
          serverList2.get(0).getHost().equals("localhost")
              && serverList2.get(0).getPort() == 10003
              && serverList2.get(1).getHost().equals("localhost")
              && serverList2.get(1).getPort() == 10004
              && serverList2.get(2).getHost().equals("localhost")
              && serverList2.get(2).getPort() == 10005);

      client.leave(gid1);
      check(client, new HashSet<>(Collections.singletonList(gid2)));
      queryResult[4] = client.query(-1);

      client.leave(gid2);
      queryResult[5] = client.query(-1);
    }
    System.out.println("  ... Passed");

    System.out.println("Test: Historical queries ...");
    {
      for (int i = 0; i < config.getnMasters(); i++) {
        config.shutDownMaster(i);
        for (String s : queryResult) {
          final String res = client.query(getVersion(s));
          assertEquals(s, res);
        }
        config.startMaster(i);
      }
    }
    System.out.println("  ... Passed");

    System.out.println("Test: Move ...");
    {
      long gid3 = 503;
      client.join(
          gid3,
          Arrays.asList(
              new Pair<>("localhost", 20000),
              new Pair<>("localhost", 20001),
              new Pair<>("localhost", 20002)));
      long gid4 = 504;
      client.join(
          gid4,
          Arrays.asList(
              new Pair<>("localhost", 20003),
              new Pair<>("localhost", 20004),
              new Pair<>("localhost", 20005)));

      for (int i = 0; i < MasterConfig.SHARD_COUNT; i++) {
        final String res1 = client.query(-1);

        if (i < MasterConfig.SHARD_COUNT / 2) {
          client.move(i, gid3);
          if (getContainingGroup(res1, i) != gid3) {
            final String res2 = client.query(-1);
            assertTrue(getVersion(res2) > getVersion(res1));
          }
        } else {
          client.move(i, gid4);
          if (getContainingGroup(res1, i) != gid4) {
            final String res2 = client.query(-1);
            if (getVersion(res2) <= getVersion(res1)) {
              System.out.println(2333);
            }
            assertTrue(getVersion(res2) > getVersion(res1));
          }
        }
      }

      final String res3 = client.query(-1);
      for (int i = 0; i < MasterConfig.SHARD_COUNT; i++) {
        if (i < MasterConfig.SHARD_COUNT / 2) {
          assertEquals(getContainingGroup(res3, i), gid3);
        } else {
          assertEquals(getContainingGroup(res3, i), gid4);
        }
      }
      client.leave(gid3);
      client.leave(gid4);
    }
    System.out.println("  ... Passed");

    System.out.println("Test: Concurrent leave/join ...");
    {
      MasterClient[] clients = new MasterClient[nParallelClient];

      for (int i = 0; i < nParallelClient; i++) {
        clients[i] = config.createClient();
      }

      Thread[] threads = new Thread[nParallelClient];
      Set<Long> gids = new ConcurrentSkipListSet<>();
      for (int i = 0; i < nParallelClient; i++) {
        final int finalI = i;
        threads[i] =
            new Thread(
                () -> {
                  clients[finalI].join(
                      finalI + 1000,
                      Arrays.asList(
                          new Pair<>("localhost", 30000 + finalI * 10),
                          new Pair<>("localhost", 30001 + finalI * 10),
                          new Pair<>("localhost", 30002 + finalI * 10)));
                  clients[finalI].join(
                      finalI + 2000,
                      Arrays.asList(
                          new Pair<>("localhost", 40000 + finalI * 10),
                          new Pair<>("localhost", 40001 + finalI * 10),
                          new Pair<>("localhost", 40002 + finalI * 10)));
                  clients[finalI].leave(finalI + 2000);
                  gids.add(finalI + 1000L);
                });
        threads[i].start();
      }
      for (int i = 0; i < nParallelClient; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      check(client, gids);
    }
    System.out.println("  ... Passed");
  }
}
