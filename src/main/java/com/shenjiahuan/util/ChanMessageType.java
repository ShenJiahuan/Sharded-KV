package com.shenjiahuan.util;

public enum ChanMessageType {
  SUCCESS(0),
  TIMEOUT(1);

  private int code;

  ChanMessageType(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static ChanMessageType convert(int code) {
    ChanMessageType[] enums = ChanMessageType.values();
    for (ChanMessageType e : enums) {
      if (e.code == code) return e;
    }
    return null;
  }
}
