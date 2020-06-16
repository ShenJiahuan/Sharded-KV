package com.shenjiahuan.zookeeper;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.shenjiahuan.log.Log;
import com.shenjiahuan.node.GenericNode;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class ZKConnection implements AsyncCallback.StatCallback {

  private final Logger logger = Logger.getLogger(getClass());

  private ZooKeeper zoo;
  private final String host;
  private final GenericNode listener;
  private AtomicBoolean dead = new AtomicBoolean(false);
  private final String znode;
  private byte[] prevData;
  private final Map<String, Integer> znodeVer = new ConcurrentHashMap<>();
  private AtomicBoolean isLeader = new AtomicBoolean(false);
  private String createdNode;
  private String watchedNode;
  private Lock mutex = new ReentrantLock();

  private final String leaderElectionRootNode;
  private static final String PROCESS_NODE_PREFIX = "/p_";

  public ZKConnection(
      String host, GenericNode listener, String znode, String leaderElectionRootNode) {
    this.host = host;
    this.listener = listener;
    this.znode = znode;
    this.leaderElectionRootNode = leaderElectionRootNode;
  }

  public void connect() throws IOException {
    zoo =
        new ZooKeeper(
            host,
            15000,
            event -> {
              String path = event.getPath();
              if (event.getType() == Watcher.Event.EventType.None) {
                // We are are being told that the state of the
                // connection has changed
                switch (event.getState()) {
                  case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                  case Expired:
                    // It's all over
                    dead.set(true);

                    break;
                }
              } else {
                if (path != null && path.equals(znode)) {
                  // Something has changed on the node, let's find out
                  zoo.exists(znode, true, this, null);
                }
              }
            });

    new Thread(this::leaderElection).start();

    zoo.exists(znode, true, this, null);
  }

  @Override
  public void processResult(int rc, String path, Object ctx, Stat stat) {
    boolean exists;
    switch (KeeperException.Code.get(rc)) {
      case OK:
        exists = true;
        break;
      case NONODE:
        exists = false;
        break;
      case SESSIONEXPIRED:
      case NOAUTH:
        return;
      default:
        // Retry errors
        zoo.exists(znode, true, this, null);
        return;
    }

    byte[] b;
    if (exists) {
      try {
        b = zoo.getData(znode, false, null);
      } catch (KeeperException | InterruptedException e) {
        return;
      }

      mutex.lock();
      znodeVer.put(znode, stat.getVersion());
      if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {

        List<Log> currentLog =
            b == null || b.length == 0
                ? new ArrayList<>()
                : new Gson().fromJson(new String(b), new TypeToken<List<Log>>() {}.getType());
        List<Log> prevLog =
            prevData == null || prevData.length == 0
                ? new ArrayList<>()
                : new Gson()
                    .fromJson(new String(prevData), new TypeToken<List<Log>>() {}.getType());
        List<Log> logDiff = new ArrayList<>(currentLog.subList(prevLog.size(), currentLog.size()));

        if (logDiff.size() > 0) {
          listener.handleChange(logDiff, 0);
        }
        prevData = b;
      }
      //      logger.info("prevData length: " + (prevData == null ? 0 : prevData.length));
      mutex.unlock();
    }
  }

  public boolean isDead() {
    return dead.get();
  }

  public void append(JsonObject data) {
    while (true) {
      try {
        mutex.lock();
        JsonArray array =
            prevData == null || prevData.length == 0
                ? new JsonArray()
                : JsonParser.parseString(new String(prevData)).getAsJsonArray();
        array.add(data);
        try {
          zoo.setData(znode, array.toString().getBytes(), znodeVer.get(znode));
        } catch (KeeperException.SessionExpiredException e) {
          logger.warn("Session expired, failed to put into " + znode);
        } catch (KeeperException.ConnectionLossException e) {
          logger.warn("Connection lost, failed to put into " + znode);
        }
        mutex.unlock();
        return;
      } catch (KeeperException.BadVersionException e) {
        logger.info("version incorrect, will retry later...");
        mutex.unlock();
        Thread.yield();
      } catch (KeeperException | InterruptedException e) {
        mutex.unlock();
        e.printStackTrace();
      }
    }
  }

  public void leaderElection() {
    try {
      createdNode =
          zoo.create(
              leaderElectionRootNode + PROCESS_NODE_PREFIX,
              new byte[0],
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              EPHEMERAL_SEQUENTIAL);
      doLeaderElection();
    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void doLeaderElection() {
    final List<String> childNodes;
    try {
      childNodes = zoo.getChildren(leaderElectionRootNode, false);
      final long createdIndex =
          Long.parseLong(
              createdNode.substring(
                  leaderElectionRootNode.length() + PROCESS_NODE_PREFIX.length()));
      final List<Long> childIndexes =
          childNodes
              .stream()
              .map(node -> Long.parseLong(node.substring(PROCESS_NODE_PREFIX.length())))
              .sorted()
              .collect(Collectors.toList());

      if (childIndexes.indexOf(createdIndex) == 0) {
        logger.info("I am the leader");
        isLeader.set(true);
      } else {
        final long watchedIndex = childIndexes.get(childIndexes.indexOf(createdIndex) - 1);
        watchedNode =
            leaderElectionRootNode
                + "/"
                + childNodes
                    .stream()
                    .filter(
                        node ->
                            Long.parseLong(node.substring(PROCESS_NODE_PREFIX.length()))
                                == watchedIndex)
                    .collect(Collectors.toList())
                    .get(0);
        zoo.exists(
            watchedNode,
            event -> {
              final Watcher.Event.EventType eventType = event.getType();
              if (Watcher.Event.EventType.NodeDeleted.equals(eventType)) {
                assert event.getPath().equals(watchedNode);
                doLeaderElection();
              }
            });
      }
    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean isLeader() {
    return isLeader.get();
  }

  public void close() {
    try {
      logger.info("Zookeeper connection will be closed");
      zoo.close();
      logger.info("Zookeeper connection closed");
      dead.set(true);
    } catch (InterruptedException e) {
      close();
    }
  }
}
