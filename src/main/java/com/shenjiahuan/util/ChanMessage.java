package com.shenjiahuan.util;

public class ChanMessage<T> {

  private ChanMessageType type;
  private T data;

  public ChanMessage(ChanMessageType type, T data) {
    this.type = type;
    this.data = data;
  }

  public ChanMessageType getType() {
    return type;
  }

  public void setType(ChanMessageType type) {
    this.type = type;
  }

  public T getData() {
    return data;
  }

  public void setData(T data) {
    this.data = data;
  }

  @Override
  public String toString() {
    return "ChanMessage{" + "type=" + type + ", data=" + data + '}';
  }
}
