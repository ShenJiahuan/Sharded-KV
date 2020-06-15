package com.shenjiahuan.log;

public enum Action {
  JOIN(1),
  LEAVE(2),
  QUERY(3),
  MOVE(4),
  GET(5),
  PUT_OR_DELETE(6),
  UPDATE_CONF(7),
  PULL(8);

  private int code;

  Action(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static Action convert(int code) {
    Action[] enums = Action.values();
    for (Action e : enums) {
      if (e.code == code) return e;
    }
    return null;
  }
}
