package com.shenjiahuan.node;

import com.shenjiahuan.log.Log;
import java.util.List;

public abstract class GenericNode {

  public abstract void handleChange(List<Log> newLogs);

  public abstract void close();
}