package com.shenjiahuan.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.shenjiahuan.Server;
import com.shenjiahuan.config.MasterConfig;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;

public class Utils {
  public static long key2Shard(String key) {
    if (key == null) {
      throw new RuntimeException("Invalid argument");
    }

    return Integer.parseInt(DigestUtils.md5Hex(key).substring(0, 4), 16) % MasterConfig.SHARD_COUNT;
  }

  public static Map<Long, List<Server>> getGroupMap(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return new Gson()
        .fromJson(
            jsonObject.get("groupMap").toString(),
            new TypeToken<Map<Long, List<Server>>>() {}.getType());
  }

  public static Map<Long, List<Long>> getShardMap(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return new Gson()
        .fromJson(
            jsonObject.get("shardMap").toString(),
            new TypeToken<Map<Long, List<Long>>>() {}.getType());
  }

  public static long getContainingGroup(String queryResponse, long shardId) {
    return getContainingGroup(getShardMap(queryResponse), shardId);
  }

  public static long getContainingGroup(Map<Long, List<Long>> shardMap, long shardId) {
    if (shardMap.entrySet().stream().noneMatch(e -> e.getValue().contains(shardId))) {
      return -1;
    }
    return shardMap
        .entrySet()
        .stream()
        .filter(e -> e.getValue().contains(shardId))
        .collect(Collectors.toList())
        .get(0)
        .getKey();
  }

  public static boolean shardExists(Map<Long, List<Long>> shardMap, long shardId) {
    return shardMap.entrySet().stream().anyMatch(e -> e.getValue().contains(shardId));
  }

  public static int getVersion(String queryResponse) {
    final JsonObject jsonObject = JsonParser.parseString(queryResponse).getAsJsonObject();
    return jsonObject.get("version").getAsInt();
  }

  public static <K, V> Map<K, V> copy(Map<K, V> m) {
    return m.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static BlockingQueue<ChanMessage<NotifyResponse>> createChan(int timeout) {
    BlockingQueue<ChanMessage<NotifyResponse>> notifyChan = new ArrayBlockingQueue<>(1);
    new Thread(
            () -> {
              Utils.sleep(timeout);
              notifyChan.offer(new ChanMessage<>(ChanMessageType.TIMEOUT, null));
            })
        .start();
    return notifyChan;
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
