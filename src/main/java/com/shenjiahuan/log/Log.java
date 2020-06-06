package com.shenjiahuan.log;

import java.util.Objects;

public class Log {

  private Action action;
  private String data;

  public Log(Action action, String data) {
    this.action = action;
    this.data = data;
  }

  public Action getAction() {
    return action;
  }

  public void setAction(Action action) {
    this.action = action;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Log log = (Log) o;
    return action == log.action && data.equals(log.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(action, data);
  }

  @Override
  public String toString() {
    return "Log{" + "action=" + action + ", data='" + data + '\'' + '}';
  }
}
