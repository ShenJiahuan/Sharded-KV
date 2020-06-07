package com.shenjiahuan.node;

import com.shenjiahuan.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class MasterClient {

  private final Logger logger = Logger.getLogger(getClass());

  private final List<Pair<String, Integer>> masters;
  private int leader;
  private final long clientId;
  private long seqId;

  public MasterClient(List<Pair<String, Integer>> masters) {
    this.masters = masters;
    this.leader = 0;
    this.clientId = new Random().nextLong();
    this.seqId = 0;
  }

  public void join(long gid, List<Pair<String, Integer>> slaves) {
    while (true) {
      final String masterHost = masters.get(leader).getKey();
      final Integer masterPort = masters.get(leader).getValue();
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();

      MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);

      ServerList.Builder builder = ServerList.newBuilder();
      for (Pair<String, Integer> slave : slaves) {
        final String slaveHost = slave.getKey();
        final Integer slavePort = slave.getValue();
        builder.addServer(Server.newBuilder().setHost(slaveHost).setPort(slavePort).build());
      }

      JoinResponse joinResponse;
      try {
        joinResponse =
            stub.join(
                JoinRequest.newBuilder()
                    .setGid(gid)
                    .setServer(builder.build())
                    .setClientId(clientId)
                    .setSeqId(seqId)
                    .build());

        logger.info("Response received from server:\n" + joinResponse);
        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        ;
      } catch (StatusRuntimeException e) {
        logger.info("Fail to get response from server");
        this.leader = (this.leader + 1) % masters.size();
        continue;
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      if (joinResponse.getStatus() == 0) {
        seqId++;
        return;
      } else {
        this.leader = (this.leader + 1) % masters.size();
      }
    }
  }

  public void leave(long gid) {
    while (true) {
      final String masterHost = masters.get(leader).getKey();
      final Integer masterPort = masters.get(leader).getValue();
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();

      MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);

      LeaveResponse leaveResponse;
      try {
        leaveResponse =
            stub.leave(
                LeaveRequest.newBuilder()
                    .setGid(gid)
                    .setClientId(clientId)
                    .setSeqId(seqId)
                    .build());

        logger.info("Response received from server:\n" + leaveResponse);
        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        ;
      } catch (StatusRuntimeException e) {
        logger.info("Fail to get response from server");
        this.leader = (this.leader + 1) % masters.size();
        continue;
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      if (leaveResponse.getStatus() == 0) {
        seqId++;
        return;
      } else {
        this.leader = (this.leader + 1) % masters.size();
      }
    }
  }

  public String query(int version) {
    while (true) {
      final String masterHost = masters.get(leader).getKey();
      final Integer masterPort = masters.get(leader).getValue();
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();

      MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);

      QueryResponse queryResponse;
      try {
        queryResponse =
            stub.query(
                QueryRequest.newBuilder()
                    .setVersion(version)
                    .setClientId(clientId)
                    .setSeqId(seqId)
                    .build());

        logger.info("Response received from server:\n" + queryResponse);
      } catch (StatusRuntimeException e) {
        logger.info("Fail to get response from server");
        this.leader = (this.leader + 1) % masters.size();
        continue;
      } finally {
        try {
          channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      if (queryResponse.getStatus() == 0) {
        seqId++;
        return queryResponse.getData();
      } else {
        this.leader = (this.leader + 1) % masters.size();
      }
    }
  }

  public void move(long shardId, long gid) {
    while (true) {
      final String masterHost = masters.get(leader).getKey();
      final Integer masterPort = masters.get(leader).getValue();
      ManagedChannel channel =
          ManagedChannelBuilder.forAddress(masterHost, masterPort).usePlaintext().build();

      MasterServiceGrpc.MasterServiceBlockingStub stub = MasterServiceGrpc.newBlockingStub(channel);

      MoveResponse moveResponse;
      try {
        moveResponse =
            stub.move(
                MoveRequest.newBuilder()
                    .setShardId(shardId)
                    .setGid(gid)
                    .setClientId(clientId)
                    .setSeqId(seqId)
                    .build());

        logger.info("Response received from server:\n" + moveResponse);
        channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        ;
      } catch (StatusRuntimeException e) {
        logger.info("Fail to get response from server");
        this.leader = (this.leader + 1) % masters.size();
        continue;
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      if (moveResponse.getStatus() == 0) {
        seqId++;
        return;
      } else {
        this.leader = (this.leader + 1) % masters.size();
      }
    }
  }
}
