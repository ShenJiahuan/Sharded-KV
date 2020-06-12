package com.shenjiahuan.node;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.shenjiahuan.Server;
import com.shenjiahuan.ServerList;
import com.shenjiahuan.config.MasterConfig;
import com.shenjiahuan.log.Action;
import com.shenjiahuan.log.Log;
import com.shenjiahuan.rpc.MasterGrpcServer;
import com.shenjiahuan.util.Pair;
import com.shenjiahuan.zookeeper.ZKConnection;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class Master extends GenericNode implements Runnable {

  private final Logger logger = Logger.getLogger(getClass());
  private ZKConnection conn;
  private final MasterConfig masterConfig = new MasterConfig();
  private final String url;
  private final int masterPort;
  private final Map<Long, Long> lastExecuted = new ConcurrentHashMap<>();

  public Master(String url, int masterPort) {
    this.url = url;
    this.masterPort = masterPort;
  }

  public void handleChange(List<Log> newLogs, int index) {
    // TODO: What if same client send concurrent requests?
    for (Log log : newLogs) {
      JsonObject logData = JsonParser.parseString(log.getData()).getAsJsonObject();
      switch (log.getAction()) {
        case JOIN:
          {
            final long gid = logData.get("gid").getAsLong();
            final List<Server> serverList =
                new Gson().fromJson(logData.get("serverList"), ServerList.class).getServerList();
            final Long clientId = logData.get("clientId").getAsLong();
            final Long seqId = logData.get("seqId").getAsLong();
            final Long last = lastExecuted.get(clientId);
            if (last == null || last < seqId) {
              handleJoin(gid, serverList);
              lastExecuted.put(clientId, seqId);
            }
            break;
          }
        case LEAVE:
          {
            final long gid = logData.get("gid").getAsLong();
            final Long clientId = logData.get("clientId").getAsLong();
            final Long seqId = logData.get("seqId").getAsLong();
            final Long last = lastExecuted.get(clientId);
            if (last == null || last < seqId) {
              handleLeave(gid);
              lastExecuted.put(clientId, seqId);
            }
            break;
          }
        case QUERY:
          {
            final Long clientId = logData.get("clientId").getAsLong();
            final Long seqId = logData.get("seqId").getAsLong();
            final Long last = lastExecuted.get(clientId);
            if (last == null || last < seqId) {
              lastExecuted.put(clientId, seqId);
            }
            break;
          }
        case MOVE:
          {
            final Long shardId = logData.get("shardId").getAsLong();
            final Long gid = logData.get("gid").getAsLong();
            final Long clientId = logData.get("clientId").getAsLong();
            final Long seqId = logData.get("seqId").getAsLong();
            final Long last = lastExecuted.get(clientId);
            if (last == null || last < seqId) {
              handleMove(shardId, gid);
              lastExecuted.put(clientId, seqId);
            }
            break;
          }
        default:
          throw new RuntimeException("Unhandled action");
      }
    }
  }

  public void handleJoin(long gid, List<Server> serverList) {
    masterConfig.addGroup(gid, serverList);
  }

  public void handleLeave(long gid) {
    masterConfig.removeGroup(gid);
  }

  public void handleMove(long shardId, long gid) {
    masterConfig.moveShard(shardId, gid);
  }

  public int join(Long gid, ServerList serverList, Long clientId, Long seqId) {
    if (!conn.isLeader()) {
      return 1;
    }

    logger.info(Thread.currentThread().getId() + ": group " + gid + " joined");

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("gid", gid);
    jsonData.add("serverList", new Gson().toJsonTree(serverList));
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    conn.append(new Gson().toJsonTree(new Log(Action.JOIN, data)).getAsJsonObject());

    return 0;
  }

  public int leave(Long gid, Long clientId, Long seqId) {
    if (!conn.isLeader()) {
      return 1;
    }

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("gid", gid);
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    conn.append(new Gson().toJsonTree(new Log(Action.LEAVE, data)).getAsJsonObject());
    return 0;
  }

  // TODO: Do I handle resent quries correctly? They will be logged twice.
  public Pair<Integer, String> query(int version, Long clientId, Long seqId) {
    if (!conn.isLeader()) {
      return new Pair<>(1, "");
    }

    if (version < 0 || version >= masterConfig.getCurrentVersion()) {
      final JsonObject jsonData = new JsonObject();
      jsonData.addProperty("version", version);
      jsonData.addProperty("clientId", clientId);
      jsonData.addProperty("seqId", seqId);
      final String data = jsonData.toString();
      conn.append(new Gson().toJsonTree(new Log(Action.QUERY, data)).getAsJsonObject());
    }
    final String config = masterConfig.getConfig(version);
    return new Pair<>(0, config);
  }

  public int move(Long shardId, Long gid, Long clientId, Long seqId) {
    if (!conn.isLeader()) {
      return 1;
    }

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("shardId", shardId);
    jsonData.addProperty("gid", gid);
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    conn.append(new Gson().toJsonTree(new Log(Action.MOVE, data)).getAsJsonObject());
    return 0;
  }

  @Override
  public void close() {
    conn.close();
    synchronized (this) {
      notifyAll();
    }
  }

  public static void main(String[] args) {
    assert args.length == 2;
    final String url = args[0];
    final int masterPort = Integer.parseInt(args[1]);
    new Master(url, masterPort).run();
  }

  @Override
  public void run() {
    final MasterGrpcServer grpcServer = new MasterGrpcServer(this, masterPort);
    grpcServer.start();
    conn = new ZKConnection(url, this, "/test", "/election/master");
    try {
      conn.connect();
      synchronized (this) {
        while (!conn.isDead()) {
          wait();
        }
      }
      grpcServer.close();
      while (!grpcServer.isTerminated()) {
        Thread.yield();
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
