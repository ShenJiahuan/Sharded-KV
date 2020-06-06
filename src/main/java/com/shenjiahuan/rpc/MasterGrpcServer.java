package com.shenjiahuan.rpc;

import com.shenjiahuan.node.Master;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.log4j.Logger;

public class MasterGrpcServer extends Thread {

  private final Logger logger = Logger.getLogger(getClass());

  private final Master master;
  private final int port;
  private Server server;

  public MasterGrpcServer(Master master, int port) {
    super();
    this.master = master;
    this.port = port;
  }

  @Override
  public void run() {
    server = ServerBuilder.forPort(port).addService(new MasterServiceImpl(master)).build();

    try {
      server.start();
      logger.info("RPC server started for master");
      server.awaitTermination();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    server.shutdownNow();
  }

  public boolean isTerminated() {
    return server.isTerminated();
  }
}
