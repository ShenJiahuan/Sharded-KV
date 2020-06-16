package com.shenjiahuan.node;

import com.shenjiahuan.*;
import com.shenjiahuan.util.Pair;
import com.shenjiahuan.util.StatusCode;
import com.shenjiahuan.util.Utils;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public class ShardClient {

  private final Logger logger = Logger.getLogger(getClass());

  private final long clientId;
  private long requestId;
  private Map<Long, List<Server>> groupMap = new HashMap<>();
  private Map<Long, List<Long>> shardMap = new HashMap<>();
  private int version = -1;
  private final Map<Long, Integer> groupLeader = new HashMap<>();
  private boolean initialized = false;
  private final MasterClient masterClient;
  private final Lock mutex = new ReentrantLock();
  private static final Long RETRY_INTERVAL = 10L;
  private final AtomicBoolean aborted = new AtomicBoolean(false);

  public ShardClient(List<Pair<String, Integer>> masters) {
    this.clientId = new Random().nextLong();
    this.requestId = 0;
    this.masterClient = new MasterClient(masters);
  }

  private StatusCode doPutOrDelete(String key, String value, boolean delete, Server server) {
    final String host = server.getHost();
    final int port = server.getPort();
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

    ShardServiceGrpc.ShardServiceBlockingStub stub = ShardServiceGrpc.newBlockingStub(channel);

    PutOrDeleteResponse putOrDeleteResponse;
    try {
      putOrDeleteResponse =
          stub.put(
              PutOrDeleteRequest.newBuilder()
                  .setClientVersion(version)
                  .setKey(key)
                  .setValue(value)
                  .setDelete(delete)
                  .setClientId(clientId)
                  .setRequestId(requestId)
                  .build());

      //      logger.info("Response received from server:\n" + putOrDeleteResponse);
    } catch (StatusRuntimeException e) {
      logger.info("Fail to get response from server");
      return StatusCode.CONNECTION_LOST;
    } finally {
      try {
        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return StatusCode.convert(putOrDeleteResponse.getStatus());
  }

  public Pair<StatusCode, String> doGet(String key, Server server) {
    final String host = server.getHost();
    final int port = server.getPort();
    ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

    ShardServiceGrpc.ShardServiceBlockingStub stub = ShardServiceGrpc.newBlockingStub(channel);

    GetResponse getResponse;
    try {
      getResponse = stub.get(GetRequest.newBuilder().setClientVersion(version).setKey(key).build());

      //      logger.info("Response received from server:\n" + getResponse);
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

    return new Pair<>(StatusCode.convert(getResponse.getStatus()), getResponse.getValue());
  }

  public void init() {
    if (initialized) {
      throw new RuntimeException("Client has already been initialized");
    }

    initialized = true;
    updateConfig(-1);
  }

  public void updateConfig(int version) {
    final String masterConfig = masterClient.query(version);
    if (version == -1 || Utils.getVersion(masterConfig) == version) {
      this.groupMap = Utils.getGroupMap(masterConfig);
      this.shardMap = Utils.getShardMap(masterConfig);
      this.groupMap.keySet().forEach(gid -> groupLeader.put(gid, 0));
      this.version = Utils.getVersion(masterConfig);
    }
  }

  public void waitUntilShardExist(long shardId) {
    long wait = 10L;
    while (!Utils.shardExists(shardMap, shardId)) {
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      wait = Math.max(wait * 2, 1000);
      updateConfig(-1);
    }
  }

  public void putOrDelete(String key, String value, boolean delete) {
    aborted.set(false);
    while (!aborted.get()) {
      mutex.lock();
      try {
        final long shardId = Utils.key2Shard(key);
        waitUntilShardExist(shardId);

        final long gid = Utils.getContainingGroup(shardMap, shardId);

        final List<Server> servers = groupMap.get(gid);
        int leader = groupLeader.get(gid);
        final Server server = servers.get(leader);
        final StatusCode statusCode = doPutOrDelete(key, value, delete, server);
        logger.info(statusCode);
        if (statusCode == StatusCode.OK) {
          requestId++;
          return;
        } else if (statusCode == StatusCode.NOT_BELONG_TO
            || statusCode == StatusCode.CONNECTION_LOST) {
          try {
            Thread.sleep(RETRY_INTERVAL);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          updateConfig(version + 1);
          final boolean groupChanged = Utils.getContainingGroup(shardMap, shardId) != gid;
          if (!groupChanged) {
            leader = (leader + 1) % servers.size();
            groupLeader.put(gid, leader);
          }
        } else {
          leader = (leader + 1) % servers.size();
          groupLeader.put(gid, leader);
        }
      } finally {
        mutex.unlock();
      }
    }
  }

  public String get(String key) {
    aborted.set(false);
    while (!aborted.get()) {
      mutex.lock();
      try {
        final long shardId = Utils.key2Shard(key);
        waitUntilShardExist(shardId);

        final long gid = Utils.getContainingGroup(shardMap, shardId);

        final List<Server> servers = groupMap.get(gid);
        int leader = groupLeader.get(gid);
        final Server server = servers.get(leader);
        final Pair<StatusCode, String> result = doGet(key, server);
        final StatusCode statusCode = result.getKey();
        if (statusCode == StatusCode.OK) {
          return result.getValue();
        } else if (statusCode == StatusCode.NOT_FOUND) {
          return null;
        } else if (statusCode == StatusCode.NOT_BELONG_TO) {
          try {
            Thread.sleep(RETRY_INTERVAL);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          updateConfig(version + 1);
        } else if (statusCode == StatusCode.CONNECTION_LOST) {
          try {
            Thread.sleep(RETRY_INTERVAL);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          updateConfig(version + 1);
          final boolean groupChanged = Utils.getContainingGroup(shardMap, shardId) != gid;
          if (!groupChanged) {
            leader = (leader + 1) % servers.size();
            groupLeader.put(gid, leader);
          }
        } else {
          leader = (leader + 1) % servers.size();
          groupLeader.put(gid, leader);
        }
      } finally {
        mutex.unlock();
      }
    }
    return "";
  }

  public void abort() {
    aborted.set(true);
  }

  public void interactive() {
    final Scanner scanner = new Scanner(System.in);
    while (true) {
      System.out.print(">>> ");
      final String line = scanner.nextLine();
      final String[] parts = line.split(" ");
      switch (parts[0]) {
        case "put":
          {
            final String key = parts[1];
            final String value = parts[2];
            putOrDelete(key, value, false);
            break;
          }
        case "get":
          {
            final String key = parts[1];
            final String value = get(key);
            System.out.println(value);
            break;
          }
        case "delete":
          {
            final String key = parts[1];
            putOrDelete(key, "", true);
            break;
          }
        default:
          {
            System.out.println("Invalid operation");
            break;
          }
      }
    }
  }
}
