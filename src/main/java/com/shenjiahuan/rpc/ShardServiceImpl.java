package com.shenjiahuan.rpc;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.shenjiahuan.*;
import com.shenjiahuan.node.ShardServer;
import com.shenjiahuan.util.Pair;
import com.shenjiahuan.util.StatusCode;
import io.grpc.stub.StreamObserver;
import java.util.List;
import org.apache.log4j.Logger;

public class ShardServiceImpl extends ShardServiceGrpc.ShardServiceImplBase {

  private final Logger logger = Logger.getLogger(getClass());

  private final ShardServer shardServer;

  public ShardServiceImpl(ShardServer shardServer) {
    super();
    this.shardServer = shardServer;
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    final int clientVersion = request.getClientVersion();
    final String key = request.getKey();
    final String value = request.getValue();
    final long clientId = request.getClientId();
    final long requestId = request.getRequestId();

    final StatusCode statusCode = shardServer.put(clientVersion, key, value, clientId, requestId);

    PutResponse response = PutResponse.newBuilder().setStatus(statusCode.getCode()).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    final int clientVersion = request.getClientVersion();
    final String key = request.getKey();

    final Pair<StatusCode, String> res = shardServer.get(clientVersion, key);
    final StatusCode statusCode = res.getKey();
    final String value = res.getValue();

    GetResponse response =
        GetResponse.newBuilder().setStatus(statusCode.getCode()).setValue(value).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
    final int version = request.getVersion();
    final List<Long> shards =
        new Gson().fromJson(request.getShards(), new TypeToken<List<Long>>() {}.getType());

    final Pair<StatusCode, String> res = shardServer.migrate(version, shards);
    final StatusCode statusCode = res.getKey();
    final String data = res.getValue();

    PullResponse response =
        PullResponse.newBuilder().setStatus(statusCode.getCode()).setData(data).build();

    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
