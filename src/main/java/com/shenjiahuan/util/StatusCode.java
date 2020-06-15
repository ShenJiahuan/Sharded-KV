package com.shenjiahuan.util;

public enum StatusCode {
  OK(0),
  NOT_LEADER(1),
  NOT_BELONG_TO(2),
  CONNECTION_LOST(3),
  NOT_FOUND(4);

  private int code;

  StatusCode(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static StatusCode convert(int code) {
    StatusCode[] enums = StatusCode.values();
    for (StatusCode e : enums) {
      if (e.code == code) return e;
    }
    return null;
  }
}
