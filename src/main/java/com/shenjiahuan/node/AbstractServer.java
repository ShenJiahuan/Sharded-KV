package com.shenjiahuan.node;

import static com.shenjiahuan.util.StatusCode.NOT_BELONG_TO;
import static com.shenjiahuan.util.StatusCode.NOT_LEADER;

import com.google.gson.JsonObject;
import com.shenjiahuan.log.Log;
import com.shenjiahuan.util.ChanMessage;
import com.shenjiahuan.util.NotifyResponse;
import com.shenjiahuan.util.Utils;
import com.shenjiahuan.zookeeper.ZKConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public abstract class AbstractServer {

  protected final Logger logger = Logger.getLogger(getClass());
  protected final String url;
  protected ZKConnection conn;
  protected final Map<Long, Long> executed = new HashMap<>();
  protected final Lock mutex = new ReentrantLock();
  protected final Map<Integer, BlockingQueue<ChanMessage<NotifyResponse>>> chanMap =
      new HashMap<>();

  public AbstractServer(String url) {
    this.url = url;
  }

  public abstract void handleChange(List<Log> newLogs, int index);

  public abstract void close();

  public NotifyResponse start(int clientVersion, int serverVersion, JsonObject args) {
    try {
      mutex.lock();
      if (clientVersion != serverVersion) {
        logger.info(this.hashCode() + ": not belong to");
        return new NotifyResponse(NOT_BELONG_TO, "");
      }
      if (!conn.isLeader()) {
        logger.info(this.hashCode() + ": not leader");
        return new NotifyResponse(NOT_LEADER, "");
      }
      mutex.unlock();
      int index = conn.start(args);
      mutex.lock();
      BlockingQueue<ChanMessage<NotifyResponse>> notifyChan;
      if (!chanMap.containsKey(index)) {
        notifyChan = Utils.createChan(2000);
        chanMap.put(index, notifyChan);
      } else {
        notifyChan = chanMap.get(index);
      }
      mutex.unlock();
      ChanMessage<NotifyResponse> message;
      try {
        message = notifyChan.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      switch (message.getType()) {
        case SUCCESS:
          {
            mutex.lock();
            return message.getData();
          }
        case TIMEOUT:
          {
            mutex.lock();
            logger.info("timeout for " + args);
            return new NotifyResponse(NOT_LEADER, "");
          }
        default:
          {
            throw new RuntimeException("Not handled");
          }
      }
    } finally {
      mutex.unlock();
    }
  }

  protected NotifyResponse start(JsonObject args) {
    return start(0, 0, args);
  }
}
