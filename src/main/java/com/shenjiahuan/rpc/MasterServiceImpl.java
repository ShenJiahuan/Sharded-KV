package com.shenjiahuan.rpc;

import com.shenjiahuan.*;
import com.shenjiahuan.node.Master;
import com.shenjiahuan.util.Pair;
import com.shenjiahuan.util.StatusCode;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

public class MasterServiceImpl extends MasterServiceGrpc.MasterServiceImplBase {

  private final Logger logger = Logger.getLogger(getClass());

  private final Master master;

  public MasterServiceImpl(Master master) {
    super();
    this.master = master;
  }

  @Override
  public void join(JoinRequest request, StreamObserver<JoinResponse> responseObserver) {

    final Long gid = request.getGid();
    final ServerList serverList = request.getServer();
    final Long clientId = request.getClientId();
    final Long seqId = request.getSeqId();

    for (Server server : serverList.getServerList()) {
      logger.info("gid: " + gid + ", server: " + server);
    }

    final StatusCode statusCode = master.join(gid, serverList, clientId, seqId);

    JoinResponse response = JoinResponse.newBuilder().setStatus(statusCode.getCode()).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void leave(LeaveRequest request, StreamObserver<LeaveResponse> responseObserver) {

    final Long gid = request.getGid();
    final Long clientId = request.getClientId();
    final Long seqId = request.getSeqId();

    logger.info("gid: " + gid);

    final StatusCode statusCode = master.leave(gid, clientId, seqId);

    LeaveResponse response = LeaveResponse.newBuilder().setStatus(statusCode.getCode()).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {

    final int version = request.getVersion();
    final Long clientId = request.getClientId();
    final Long seqId = request.getSeqId();

    logger.info("version: " + version);

    final Pair<StatusCode, String> result = master.query(version, clientId, seqId);
    final StatusCode statusCode = result.getKey();
    final String data = result.getValue();

    QueryResponse response =
        QueryResponse.newBuilder().setStatus(statusCode.getCode()).setData(data).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void move(MoveRequest request, StreamObserver<MoveResponse> responseObserver) {

    final Long shardId = request.getShardId();
    final Long gid = request.getGid();
    final Long clientId = request.getClientId();
    final Long seqId = request.getSeqId();

    logger.info("shardId: " + shardId + ", gid: " + gid);

    final StatusCode statusCode = master.move(shardId, gid, clientId, seqId);

    MoveResponse response = MoveResponse.newBuilder().setStatus(statusCode.getCode()).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
