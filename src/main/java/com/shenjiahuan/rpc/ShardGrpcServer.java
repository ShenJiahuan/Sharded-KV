package com.shenjiahuan.rpc;

import com.shenjiahuan.node.ShardServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.log4j.Logger;

public class ShardGrpcServer extends Thread {

  private final Logger logger = Logger.getLogger(getClass());

  private final ShardServer shardServer;
  private final int port;
  private Server server;

  public ShardGrpcServer(ShardServer shardServer, int port) {
    super();
    this.shardServer = shardServer;
    this.port = port;
  }

  @Override
  public void run() {
    server = ServerBuilder.forPort(port).addService(new ShardServiceImpl(shardServer)).build();

    try {
      server.start();
      logger.info("RPC server started for master");
      server.awaitTermination();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    try {
      server.shutdownNow().awaitTermination();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean isTerminated() {
    return server.isTerminated();
  }
}
