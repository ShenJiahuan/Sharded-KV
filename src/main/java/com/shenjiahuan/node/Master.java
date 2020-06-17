package com.shenjiahuan.node;

import static com.shenjiahuan.util.StatusCode.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.shenjiahuan.Server;
import com.shenjiahuan.ServerList;
import com.shenjiahuan.config.MasterConfig;
import com.shenjiahuan.log.Action;
import com.shenjiahuan.log.Log;
import com.shenjiahuan.rpc.MasterGrpcServer;
import com.shenjiahuan.util.*;
import com.shenjiahuan.zookeeper.ZKConnection;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Master extends AbstractServer implements Runnable {

  private final MasterConfig masterConfig = new MasterConfig();
  private final int masterPort;

  public Master(String url, int masterPort) {
    super(url);
    this.masterPort = masterPort;
  }

  public void handleChange(List<Log> newLogs, int index) {
    logger.info(this.hashCode() + ": handle change of " + index);
    try {
      mutex.lock();
      for (Log log : newLogs) {

        logger.info(this.hashCode() + " log: " + log);
        NotifyResponse response = new NotifyResponse(OK, "");
        JsonObject logData = JsonParser.parseString(log.getData()).getAsJsonObject();
        switch (log.getAction()) {
          case JOIN:
            {
              final long gid = logData.get("gid").getAsLong();
              final List<Server> serverList =
                  new Gson().fromJson(logData.get("serverList"), ServerList.class).getServerList();
              final Long clientId = logData.get("clientId").getAsLong();
              final Long seqId = logData.get("seqId").getAsLong();
              final Long last = executed.get(clientId);
              if (last == null || last < seqId) {
                handleJoin(gid, serverList);
                executed.put(clientId, seqId);
              }
              break;
            }
          case LEAVE:
            {
              final long gid = logData.get("gid").getAsLong();
              final Long clientId = logData.get("clientId").getAsLong();
              final Long seqId = logData.get("seqId").getAsLong();
              final Long last = executed.get(clientId);
              if (last == null || last < seqId) {
                handleLeave(gid);
                executed.put(clientId, seqId);
              }
              break;
            }
          case QUERY:
            {
              final Long clientId = logData.get("clientId").getAsLong();
              final Long seqId = logData.get("seqId").getAsLong();
              final int version = logData.get("version").getAsInt();
              final Long last = executed.get(clientId);
              if (last == null || last < seqId) {
                executed.put(clientId, seqId);
                response.setData(masterConfig.getConfig(version));
              }
              break;
            }
          case MOVE:
            {
              final Long shardId = logData.get("shardId").getAsLong();
              final Long gid = logData.get("gid").getAsLong();
              final Long clientId = logData.get("clientId").getAsLong();
              final Long seqId = logData.get("seqId").getAsLong();
              final Long last = executed.get(clientId);
              if (last == null || last < seqId) {
                handleMove(shardId, gid);
                executed.put(clientId, seqId);
              }
              break;
            }
          default:
            throw new RuntimeException("Unhandled action");
        }

        logger.info("master response: " + response);

        if (conn.isLeader() && newLogs.size() == 1) {
          // if newLogs.size() != 1, this is a replay, and no one is waiting for the result
          BlockingQueue<ChanMessage<NotifyResponse>> chan;
          if (chanMap.containsKey(index)) {
            chan = chanMap.get(index);
          } else {
            chan = Utils.createChan(2000);
            chanMap.put(index, chan);
          }
          chan.offer(new ChanMessage<>(ChanMessageType.SUCCESS, response));
        }
      }
    } finally {
      mutex.unlock();
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

  public StatusCode join(Long gid, ServerList serverList, Long clientId, Long seqId) {
    logger.info(this.hashCode() + ": group " + gid + " joined");

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("gid", gid);
    jsonData.add("serverList", new Gson().toJsonTree(serverList));
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    final NotifyResponse response =
        start(new Gson().toJsonTree(new Log(Action.JOIN, data)).getAsJsonObject());

    return response.getStatusCode();
  }

  public StatusCode leave(Long gid, Long clientId, Long seqId) {
    logger.info(this.hashCode() + ": group " + gid + " left");

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("gid", gid);
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    final NotifyResponse response =
        start(new Gson().toJsonTree(new Log(Action.LEAVE, data)).getAsJsonObject());

    return response.getStatusCode();
  }

  public Pair<StatusCode, String> query(int version, Long clientId, Long seqId) {
    if (version < 0 || version >= masterConfig.getCurrentVersion()) {
      final JsonObject jsonData = new JsonObject();
      jsonData.addProperty("version", version);
      jsonData.addProperty("clientId", clientId);
      jsonData.addProperty("seqId", seqId);
      final String data = jsonData.toString();
      final NotifyResponse response =
          start(new Gson().toJsonTree(new Log(Action.QUERY, data)).getAsJsonObject());
      return new Pair<>(response.getStatusCode(), response.getData());
    } else {
      final String config = masterConfig.getConfig(version);
      return new Pair<>(OK, config);
    }
  }

  public StatusCode move(Long shardId, Long gid, Long clientId, Long seqId) {

    final JsonObject jsonData = new JsonObject();
    jsonData.addProperty("shardId", shardId);
    jsonData.addProperty("gid", gid);
    jsonData.addProperty("clientId", clientId);
    jsonData.addProperty("seqId", seqId);
    final String data = jsonData.toString();
    final NotifyResponse response =
        start(new Gson().toJsonTree(new Log(Action.MOVE, data)).getAsJsonObject());
    return response.getStatusCode();
  }

  @Override
  public void close() {
    conn.close();
    synchronized (this) {
      notifyAll();
    }
  }

  @Override
  public void run() {
    final MasterGrpcServer grpcServer = new MasterGrpcServer(this, masterPort);
    grpcServer.start();
    conn = new ZKConnection(url, this, "/master", "/election/master");
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
