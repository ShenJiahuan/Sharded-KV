package com.shenjiahuan.util;

public class NotifyResponse {
  private StatusCode statusCode;
  private String data;

  public NotifyResponse(StatusCode statusCode, String data) {
    this.statusCode = statusCode;
    this.data = data;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public String getData() {
    return data;
  }

  public void setStatusCode(StatusCode statusCode) {
    this.statusCode = statusCode;
  }

  public void setData(String data) {
    this.data = data;
  }

  @Override
  public String toString() {
    return "NotifyResponse{" + "statusCode=" + statusCode + ", data='" + data + '\'' + '}';
  }
}
