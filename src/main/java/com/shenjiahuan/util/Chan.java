package com.shenjiahuan.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

public class Chan<T extends ChanMessage> {

  private final Logger logger = Logger.getLogger(getClass());

  private List<T> lists = new ArrayList<>();

  private int size;

  public Chan(int size) {
    this.size = size;
  }

  public synchronized T take() {
    while (lists.size() == 0) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
    T message = lists.remove(0);
    notifyAll();
    return message;
  }

  public synchronized void put(T message) {
    if (lists.size() >= size) {
      logger.info("Trying to put into chan, but already have: " + lists.get(0));
    }
    while (lists.size() >= size) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
    lists.add(message);
    notifyAll();
  }
}
