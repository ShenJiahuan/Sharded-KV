package com.shenjiahuan;

import com.shenjiahuan.node.MasterClient;
import com.shenjiahuan.node.ShardClient;
import com.shenjiahuan.node.ShardServer;
import com.shenjiahuan.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestShardServerConfig extends TestMasterConfig {

  private final int nGroups;
  private final int serverPerGroup;
  private final List<List<ShardServer>> shardServers;
  private final List<List<Integer>> shardServerPorts;
  private final MasterClient masterClient;

  public TestShardServerConfig(
      int nMasters,
      List<Integer> masterPorts,
      int nGroups,
      int serverPerGroup,
      List<List<Integer>> shardServerPorts) {
    super(nMasters, masterPorts);

    assert nGroups == shardServerPorts.size();
    this.nGroups = nGroups;
    this.serverPerGroup = serverPerGroup;
    this.shardServerPorts = shardServerPorts;

    this.shardServers = new ArrayList<>();
    for (int gid = 0; gid < nGroups; gid++) {
      assert serverPerGroup == shardServerPorts.get(gid).size();
      this.shardServers.add(new ArrayList<>());
      for (int i = 0; i < serverPerGroup; i++) {
        final ShardServer shardServer =
            new ShardServer(
                "localhost:21811",
                shardServerPorts.get(gid).get(i),
                gid,
                masterPorts
                    .stream()
                    .map(port -> new Pair<>("localhost", port))
                    .collect(Collectors.toList()));
        new Thread(shardServer).start();
        this.shardServers.get(gid).add(shardServer);
      }
    }
    this.masterClient =
        new MasterClient(
            masterPorts
                .stream()
                .map(port -> new Pair<>("localhost", port))
                .collect(Collectors.toList()));
  }

  public ShardClient createShardClient() {
    return new ShardClient(
        masterPorts
            .stream()
            .map(port -> new Pair<>("localhost", port))
            .collect(Collectors.toList()));
  }

  public void shutDownGroup(int gid) {
    for (ShardServer shardServer : shardServers.get(gid)) {
      if (!shardServer.isClosed()) {
        shardServer.close();
      }
    }
  }

  public void shutDownServer(int gid, int i) {
    shardServers.get(gid).get(i).close();
  }

  public void startGroup(int gid) {
    this.shardServers.set(gid, new ArrayList<>());
    for (int i = 0; i < serverPerGroup; i++) {
      final ShardServer shardServer =
          new ShardServer(
              "localhost:21811",
              shardServerPorts.get(gid).get(i),
              gid,
              masterPorts
                  .stream()
                  .map(port -> new Pair<>("localhost", port))
                  .collect(Collectors.toList()));
      new Thread(shardServer).start();
      this.shardServers.get(gid).add(shardServer);
    }
  }

  public void startServer(int gid, int i) {
    final ShardServer shardServer =
        new ShardServer(
            "localhost:21811",
            shardServerPorts.get(gid).get(i),
            gid,
            masterPorts
                .stream()
                .map(port -> new Pair<>("localhost", port))
                .collect(Collectors.toList()));
    new Thread(shardServer).start();
    this.shardServers.get(gid).set(i, shardServer);
  }

  public void joinGroup(int gid) {
    masterClient.join(
        gid,
        shardServerPorts
            .get(gid)
            .stream()
            .map(port -> new Pair<>("localhost", port))
            .collect(Collectors.toList()));
  }

  public void leaveGroup(int gid) {
    masterClient.leave(gid);
  }

  @Override
  public void cleanUp() {
    for (int i = 0; i < nGroups; i++) {
      shutDownGroup(i);
    }

    super.cleanUp();
  }
}
