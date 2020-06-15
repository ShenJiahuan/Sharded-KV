package com.shenjiahuan.node;

import static com.shenjiahuan.util.StatusCode.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.shenjiahuan.PullRequest;
import com.shenjiahuan.PullResponse;
import com.shenjiahuan.Server;
import com.shenjiahuan.ShardServiceGrpc;
import com.shenjiahuan.log.Action;
import com.shenjiahuan.log.Log;
import com.shenjiahuan.rpc.ShardGrpcServer;
import com.shenjiahuan.util.*;
import com.shenjiahuan.zookeeper.ZKShardConnection;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class ShardServer extends GenericNode implements Runnable {

  private final Logger logger = Logger.getLogger(getClass());
  private final String url;
  private ZKShardConnection conn;
  private final MasterClient masterClient;
  private final int shardServerPort;
  private final long gid;
  private int version = -1;
  private Map<Long, List<Server>> groupMap = new HashMap<>();
  private Map<Long, List<Long>> shardMap = new HashMap<>();
  private final Map<Integer, Map<Long, List<Server>>> historyGroupMap = new HashMap<>();
  private final Map<Integer, Map<Long, List<Long>>> historyShardMap = new HashMap<>();
  private Map<String, String> shardData = new HashMap<>();
  private final Map<Long, Integer> waitingShards = new HashMap<>();
  private final Map<Long, Long> executed = new HashMap<>();
  private final Map<Integer, Map<String, String>> migratingShardData = new HashMap<>();
  private final Map<Integer, Map<Long, Long>> migratingExecuted = new HashMap<>();
  private final Map<Integer, Map<Long, Integer>> migratingWaiting = new HashMap<>();
  private final Lock mutex = new ReentrantLock();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final Map<Integer, Chan<ChanMessage<NotifyResponse>>> chanMap = new HashMap<>();

  public ShardServer(
      String url, int shardServerPort, long gid, List<Pair<String, Integer>> masters) {
    this.url = url;
    this.shardServerPort = shardServerPort;
    this.gid = gid;
    this.masterClient = new MasterClient(masters);
  }

  public NotifyResponse start(int clientVersion, JsonObject args, boolean ignoreClientVersion) {
    try {
      // logger.info(this.hashCode() + ": acquiring lock");
      mutex.lock();
      // logger.info(this.hashCode() + ": acquired lock");
      if (clientVersion != version && !ignoreClientVersion) {
        logger.info(this.hashCode() + ": not belong to");
        return new NotifyResponse(NOT_BELONG_TO, "");
      }
      if (!conn.isLeader()) {
        logger.info(this.hashCode() + ": not leader");
        return new NotifyResponse(NOT_LEADER, "");
      }
      // logger.info(this.hashCode() + ": releasing lock");
      mutex.unlock();
      // logger.info(this.hashCode() + ": released lock");
      int index = conn.start(args);
      // logger.info(this.hashCode() + ": acquiring lock");
      mutex.lock();
      // logger.info(this.hashCode() + ": acquired lock");
      Chan<ChanMessage<NotifyResponse>> notifyChan;
      if (!chanMap.containsKey(index)) {
        notifyChan = Utils.createChan(2000);
        chanMap.put(index, notifyChan);
      } else {
        notifyChan = chanMap.get(index);
      }
      // logger.info(this.hashCode() + ": releasing lock");
      mutex.unlock();
      // logger.info(this.hashCode() + ": released lock");

      ChanMessage<NotifyResponse> message = notifyChan.take();
      switch (message.getType()) {
        case SUCCESS:
          {
            // logger.info(this.hashCode() + ": acquiring lock");
            mutex.lock();
            // logger.info(this.hashCode() + ": acquired lock");
            return message.getData();
          }
        case TIMEOUT:
          {
            // logger.info(this.hashCode() + ": acquiring lock");
            mutex.lock();
            // logger.info(this.hashCode() + ": acquired lock");
            logger.info("timeout for " + args);
            return new NotifyResponse(NOT_LEADER, "");
          }
        default:
          {
            throw new RuntimeException("Not handled");
          }
      }
    } finally {
      // logger.info(this.hashCode() + ": releasing lock");
      mutex.unlock();
      // logger.info(this.hashCode() + ": released lock");
    }
  }

  public NotifyResponse start(int clientVersion, JsonObject args) {
    return start(clientVersion, args, false);
  }

  public NotifyResponse start(JsonObject args) {
    return start(0, args, true);
  }

  public void handleChange(List<Log> newLogs, int index) {
    logger.info(this.hashCode() + ": handle change of " + index);
    try {
      // logger.info(this.hashCode() + ": acquiring lock");
      mutex.lock();
      // logger.info(this.hashCode() + ": acquired lock");
      for (Log log : newLogs) {

        logger.info(this.hashCode() + " log: " + log);
        NotifyResponse response = new NotifyResponse(OK, "");
        JsonObject logData = JsonParser.parseString(log.getData()).getAsJsonObject();
        switch (log.getAction()) {
          case GET:
            {
              final int clientVersion = logData.get("clientVersion").getAsInt();
              final String key = logData.get("key").getAsString();
              final long shardId = Utils.key2Shard(key);
              if (clientVersion != version) {
                response.setStatusCode(NOT_BELONG_TO);
              } else if (Utils.getContainingGroup(shardMap, shardId) != gid) {
                response.setStatusCode(NOT_BELONG_TO);
              } else if (waitingShards.size() > 0) {
                response.setStatusCode(NOT_BELONG_TO);
              } else {
                logger.info("Get " + key + " from " + gid);
                if (shardData.containsKey(key)) {
                  response.setData(shardData.get(key));
                } else {
                  response.setStatusCode(NOT_FOUND);
                  response.setData("");
                }
              }
              break;
            }
          case PUT_OR_DELETE:
            {
              final int clientVersion = logData.get("clientVersion").getAsInt();
              final String key = logData.get("key").getAsString();
              final String value = logData.get("value").getAsString();
              final boolean delete = logData.get("delete").getAsBoolean();
              final long clientId = logData.get("clientId").getAsLong();
              final long requestId = logData.get("requestId").getAsLong();
              final long shardId = Utils.key2Shard(key);
              if (clientVersion != version) {
                response.setStatusCode(NOT_BELONG_TO);
              } else if (Utils.getContainingGroup(shardMap, shardId) != gid) {
                response.setStatusCode(NOT_BELONG_TO);
              } else if (waitingShards.size() > 0) {
                response.setStatusCode(NOT_BELONG_TO);
              } else if (executed.get(clientId) == null || executed.get(clientId) != requestId) {
                if (!delete) {
                  logger.info("Put <" + key + ", " + value + "> from " + gid);
                  shardData.put(key, value);
                } else {
                  logger.info("Delete " + key + " from " + gid);
                  shardData.remove(key);
                }
                executed.put(clientId, requestId);
              }
              break;
            }
          case UPDATE_CONF:
            {
              final String config = logData.get("config").getAsString();
              handleUpdateConf(config);
              break;
            }
          case PULL:
            {
              final int pulledVersion = logData.get("version").getAsInt();
              final Map<Long, Map<String, String>> pulledShardData =
                  new Gson()
                      .fromJson(
                          logData.get("data"),
                          new TypeToken<Map<Long, Map<String, String>>>() {}.getType());
              final Map<Long, Long> pulledExecuted =
                  new Gson()
                      .fromJson(
                          logData.get("executed"), new TypeToken<Map<Long, Long>>() {}.getType());
              if (pulledVersion <= version - 1) {
                pulledShardData.forEach(
                    (key, value) -> {
                      if (waitingShards.get(key) != null
                          && pulledVersion == waitingShards.get(key)) {
                        waitingShards.remove(key);
                      }
                      migratingWaiting.forEach(
                          (prevVersion, migratingWaitingShards) -> {
                            if (migratingWaitingShards.get(key) != null
                                && pulledVersion == migratingWaitingShards.get(key)) {
                              migratingShardData.get(prevVersion).putAll(value);
                              migratingWaitingShards.remove(key);
                            }
                          });
                      shardData.putAll(value);
                    });
                executed.putAll(pulledExecuted);
              }
              break;
            }
          default:
            throw new RuntimeException("Unhandled action");
        }

        logger.info(this.hashCode() + " gid: " + gid + ", response: " + response);

        if (conn.isLeader() && newLogs.size() == 1) {
          // if newLogs.size() != 1, this is a replay, and no one is waiting for the result
          Chan<ChanMessage<NotifyResponse>> chan;
          if (chanMap.containsKey(index)) {
            chan = chanMap.get(index);
          } else {
            chan = Utils.createChan(2000);
            chanMap.put(index, chan);
          }
          chan.put(new ChanMessage<>(ChanMessageType.SUCCESS, response));
        }
      }
    } finally {
      // logger.info(this.hashCode() + ": releasing lock");
      mutex.unlock();
      // logger.info(this.hashCode() + ": released lock");
    }
  }

  private void handleUpdateConf(String config) {
    if (Utils.getVersion(config) > version) {
      final Map<Long, List<Long>> prevShardMap = shardMap;
      final int prevVersion = version;
      historyGroupMap.put(prevVersion, Utils.copy(groupMap));
      historyShardMap.put(prevVersion, Utils.copy(shardMap));
      groupMap = Utils.getGroupMap(config);
      shardMap = Utils.getShardMap(config);
      version = Utils.getVersion(config);

      final List<Long> currentShards =
          shardMap.containsKey(gid) ? shardMap.get(gid) : new ArrayList<>();

      if (prevVersion != -1) {

        for (long shardId : currentShards) {
          if (!prevShardMap.containsKey(gid) || !prevShardMap.get(gid).contains(shardId)) {
            assert !waitingShards.containsKey(shardId);
            waitingShards.put(shardId, prevVersion);
          }
        }

        if (prevShardMap.containsKey(gid)) {
          final List<Long> removedShards =
              prevShardMap
                  .get(gid)
                  .stream()
                  .filter(shardId -> !currentShards.contains(shardId))
                  .collect(Collectors.toList());

          migratingShardData.put(
              prevVersion,
              shardData
                  .entrySet()
                  .stream()
                  .filter(e -> removedShards.contains(Utils.key2Shard(e.getKey())))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

          migratingWaiting.put(
              prevVersion,
              waitingShards
                  .entrySet()
                  .stream()
                  .filter(e -> removedShards.contains(e.getKey()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

          shardData =
              shardData
                  .entrySet()
                  .stream()
                  .filter(e -> !removedShards.contains(Utils.key2Shard(e.getKey())))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        migratingExecuted.put(prevVersion, Utils.copy(executed));
      }
    }
  }

  public Pair<StatusCode, String> get(int clientVersion, String key) {
    logger.info(this.hashCode() + ": get " + key);
    JsonObject data = new JsonObject();
    data.addProperty("clientVersion", clientVersion);
    data.addProperty("key", key);
    NotifyResponse response =
        start(
            clientVersion,
            new Gson().toJsonTree(new Log(Action.GET, data.toString())).getAsJsonObject());

    return new Pair<>(response.getStatusCode(), response.getData());
  }

  public StatusCode putOrDelete(
      int clientVersion, String key, String value, boolean delete, long clientId, long requestId) {
    JsonObject data = new JsonObject();
    data.addProperty("clientVersion", clientVersion);
    data.addProperty("key", key);
    data.addProperty("value", value);
    data.addProperty("delete", delete);
    data.addProperty("clientId", clientId);
    data.addProperty("requestId", requestId);
    NotifyResponse response =
        start(
            clientVersion,
            new Gson()
                .toJsonTree(new Log(Action.PUT_OR_DELETE, data.toString()))
                .getAsJsonObject());
    return response.getStatusCode();
  }

  public Pair<StatusCode, String> migrate(int migrateVersion, List<Long> shards) {
    try {
      // logger.info(this.hashCode() + ": acquiring lock");
      mutex.lock();
      // logger.info(this.hashCode() + ": acquired lock");
      if (migrateVersion >= version) {
        return new Pair<>(NOT_BELONG_TO, "");
      }
      while (!migratingShardData.containsKey(migrateVersion)
          || migratingWaiting.get(migrateVersion).size() > 0) {
        // logger.info(this.hashCode() + ": releasing lock");
        mutex.unlock();
        // logger.info(this.hashCode() + ": released lock");
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        // logger.info(this.hashCode() + ": acquiring lock");
        mutex.lock();
        // logger.info(this.hashCode() + ": acquired lock");
      }
      assert migratingShardData
          .get(migrateVersion)
          .keySet()
          .stream()
          .allMatch(key -> shards.contains(Utils.key2Shard(key)));

      Map<Long, Map<String, String>> shardData =
          shards
              .stream()
              .collect(
                  Collectors.toMap(
                      shardId -> shardId,
                      shardId ->
                          migratingShardData
                              .get(migrateVersion)
                              .entrySet()
                              .stream()
                              .filter(e -> Utils.key2Shard(e.getKey()) == shardId)
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
      JsonObject data = new JsonObject();
      data.add("data", new Gson().toJsonTree(shardData));
      data.addProperty("version", migrateVersion);
      data.add("executed", new Gson().toJsonTree(migratingExecuted.get(migrateVersion)));

      return new Pair<>(OK, data.toString());
    } finally {
      // logger.info(this.hashCode() + ": releasing lock");
      mutex.unlock();
      // logger.info(this.hashCode() + ": released lock");
    }
  }

  private void updateConfig() {
    // logger.info(this.hashCode() + ": acquiring lock");
    mutex.lock();
    // logger.info(this.hashCode() + ": acquired lock");
    while (!stopped.get()) {
      if (conn.isLeader()) {
        final int nextVersion = version + 1;
        // logger.info(this.hashCode() + ": releasing lock");
        mutex.unlock();
        // logger.info(this.hashCode() + ": released lock");
        final String masterConfig = masterClient.query(nextVersion);
        if (Utils.getVersion(masterConfig) == nextVersion) {
          JsonObject data = new JsonObject();
          data.addProperty("config", masterConfig);
          start(
              new Gson()
                  .toJsonTree(new Log(Action.UPDATE_CONF, data.toString()))
                  .getAsJsonObject());
        }
      } else {
        // logger.info(this.hashCode() + ": releasing lock");
        mutex.unlock();
        // logger.info(this.hashCode() + ": released lock");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      mutex.lock();
    }
  }

  public Pair<StatusCode, String> doPullFromServer(int version, List<Long> shards, Server server) {
    final String host = server.getHost();
    final int port = server.getPort();
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

    ShardServiceGrpc.ShardServiceBlockingStub stub = ShardServiceGrpc.newBlockingStub(channel);

    PullResponse pullResponse;
    try {
      pullResponse =
          stub.pull(
              PullRequest.newBuilder()
                  .setVersion(version)
                  .setShards(new Gson().toJsonTree(shards).getAsJsonArray().toString())
                  .build());
    } catch (StatusRuntimeException e) {
      logger.info("Fail to get response from server");
      return new Pair<>(StatusCode.CONNECTION_LOST, null);
    } finally {
      try {
        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return new Pair<>(StatusCode.convert(pullResponse.getStatus()), pullResponse.getData());
  }

  public void doPull(int pullVersion, List<Long> shards) {
    final List<Long> gids =
        shards
            .stream()
            .map(shardId -> Utils.getContainingGroup(historyShardMap.get(pullVersion), shardId))
            .distinct()
            .collect(Collectors.toList());
    assert gids.size() == 1;
    final long gid = gids.get(0);
    final List<Server> servers = historyGroupMap.get(pullVersion).get(gid);
    for (Server server : servers) {
      final Pair<StatusCode, String> result = doPullFromServer(pullVersion, shards, server);
      if (result.getKey() == OK) {
        start(
            version,
            new Gson().toJsonTree(new Log(Action.PULL, result.getValue())).getAsJsonObject());
        return;
      }
    }
  }

  private void pull() {
    // logger.info(this.hashCode() + ": acquiring lock");
    mutex.lock();
    // logger.info(this.hashCode() + ": acquired lock");
    while (!stopped.get()) {
      if (conn.isLeader() && waitingShards.size() > 0) {
        final Map<Integer, List<Long>> shardsToPull =
            waitingShards.keySet().stream().collect(Collectors.groupingBy(waitingShards::get));
        final List<Thread> threads = new ArrayList<>();
        for (Map.Entry<Integer, List<Long>> entry : shardsToPull.entrySet()) {
          final int version = entry.getKey();
          final List<Long> shards = entry.getValue();

          final Thread t = new Thread(() -> doPull(version, shards));
          t.start();
          threads.add(t);
        }
        // logger.info(this.hashCode() + ": releasing lock");
        mutex.unlock();
        // logger.info(this.hashCode() + ": released lock");
        for (Thread t : threads) {
          try {
            t.join();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } else {
        // logger.info(this.hashCode() + ": releasing lock");
        mutex.unlock();
        // logger.info(this.hashCode() + ": released lock");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      mutex.lock();
    }
  }

  @Override
  public void close() {
    mutex.lock();
    conn.close();
    mutex.unlock();
    synchronized (this) {
      notifyAll();
    }
  }

  public boolean isClosed() {
    return stopped.get();
  }

  @Override
  public void run() {
    final ShardGrpcServer grpcServer = new ShardGrpcServer(this, shardServerPort);
    grpcServer.start();
    conn = new ZKShardConnection(url, this, "/group/" + gid, "/election/group/" + gid);
    try {
      conn.connect();
      Thread updateCfgTh = new Thread(this::updateConfig);
      Thread pullDataTh = new Thread(this::pull);
      updateCfgTh.start();
      pullDataTh.start();
      synchronized (this) {
        while (!conn.isDead()) {
          wait();
        }
      }
      grpcServer.close();
      stopped.set(true);
      updateCfgTh.join();
      pullDataTh.join();
      while (!grpcServer.isTerminated()) {
        Thread.yield();
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
