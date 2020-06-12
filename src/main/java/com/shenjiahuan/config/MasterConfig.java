package com.shenjiahuan.config;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.shenjiahuan.Server;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.log4j.Logger;

public class MasterConfig {
  private final Logger logger = Logger.getLogger(getClass());
  private final Map<Long, List<Server>> groupMap = new HashMap<>();
  private final Map<Long, List<Long>> shardMap = new HashMap<>();
  private final Lock mutex = new ReentrantLock();
  private final List<JsonObject> historyGroupMap = new ArrayList<>();
  private final List<JsonObject> historyShardMap = new ArrayList<>();

  public static final Long SHARD_COUNT = 64L;

  public void addGroup(Long gid, List<Server> serverList) {
    mutex.lock();
    if (!groupMap.containsKey(gid)) {
      groupMap.put(gid, serverList);
      reshardByAdd(gid);
      addConfigToHistory();
    } else {
      logger.warn("Attempt to add a existing group");
    }
    mutex.unlock();
  }

  public void removeGroup(Long gid) {
    mutex.lock();
    assert groupMap.containsKey(gid) == shardMap.containsKey(gid);

    if (groupMap.containsKey(gid)) {
      groupMap.remove(gid);
      reshardByRemove(gid);
      addConfigToHistory();
    } else {
      logger.warn("Attempt to remove a non-existing group");
    }
    mutex.unlock();
  }

  public void moveShard(Long shardId, Long gid) {
    mutex.lock();
    assert groupMap.containsKey(gid) == shardMap.containsKey(gid);
    assert shardId >= 0 && shardId < SHARD_COUNT;

    if (groupMap.containsKey(gid)) {
      Long fromGid =
          shardMap
              .entrySet()
              .stream()
              .filter(e -> e.getValue().contains(shardId))
              .collect(Collectors.toList())
              .get(0)
              .getKey();
      shardMap.get(fromGid).remove(shardId);
      shardMap.get(gid).add(shardId);
    } else {
      logger.warn("Attempt to move to a non-existing group");
    }
    addConfigToHistory();
    mutex.unlock();
  }

  public String getConfig(int version) {
    mutex.lock();
    assert historyGroupMap.size() == historyShardMap.size();
    if (version == -1) {
      version = historyGroupMap.size() - 1;
    }
    JsonObject jsonData = new JsonObject();
    if (version >= 0 && version < historyGroupMap.size()) {
      jsonData.addProperty("version", version);
      jsonData.add("groupMap", historyGroupMap.get(version));
      jsonData.add("shardMap", historyShardMap.get(version));
    } else {
      jsonData.addProperty("version", -2);
      jsonData.add("groupMap", new JsonObject());
      jsonData.add("shardMap", new JsonObject());
    }
    mutex.unlock();
    return jsonData.toString();
  }

  public int getCurrentVersion() {
    mutex.lock();
    final int version = historyGroupMap.size() - 1;
    mutex.unlock();
    return version;
  }

  private void reshardByAdd(Long gidToAdd) {
    if (shardMap.size() == 0) {
      shardMap.put(gidToAdd, LongStream.range(0, SHARD_COUNT).boxed().collect(Collectors.toList()));
    } else {
      Long gidAffected =
          shardMap
              .entrySet()
              .stream()
              .max(Comparator.comparingInt(e -> e.getValue().size()))
              .get()
              .getKey();
      List<Long> shards = shardMap.get(gidAffected);

      shardMap.put(gidAffected, new ArrayList<>(shards.subList(0, shards.size() / 2)));
      shardMap.put(gidToAdd, new ArrayList<>(shards.subList(shards.size() / 2, shards.size())));
    }

    logger.info("Reshard: " + Arrays.toString(shardMap.entrySet().toArray()));
  }

  private void reshardByRemove(Long gidToRemove) {
    if (shardMap.size() == 1) {
      logger.warn("Remove last group will cause service unavailable and data loss");
    } else {
      Long gidAffected =
          shardMap
              .entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(gidToRemove))
              .min(Comparator.comparingInt(e -> e.getValue().size()))
              .get()
              .getKey();

      List<Long> shards = shardMap.get(gidAffected);
      List<Long> shardsToMove = shardMap.get(gidToRemove);
      shards.addAll(shardsToMove);
    }
    shardMap.remove(gidToRemove);

    logger.info("Reshard: " + Arrays.toString(shardMap.entrySet().toArray()));
  }

  private void addConfigToHistory() {
    historyGroupMap.add(new Gson().toJsonTree(groupMap).getAsJsonObject());
    historyShardMap.add(new Gson().toJsonTree(shardMap).getAsJsonObject());
  }
}
