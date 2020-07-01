package com.shenjiahuan;

import com.shenjiahuan.node.Master;
import com.shenjiahuan.node.MasterClient;
import com.shenjiahuan.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestMasterConfig {

  private final int nMasters;
  private final List<Master> masters;
  protected final List<Integer> masterPorts;

  public TestMasterConfig(int nMasters, List<Integer> masterPorts) {
    assert masterPorts.size() == nMasters;

    this.nMasters = nMasters;
    this.masters = new ArrayList<>();
    for (int i = 0; i < nMasters; i++) {
      Master master = new Master("localhost:21811", masterPorts.get(i));
      new Thread(master).start();
      masters.add(master);
    }
    this.masterPorts = masterPorts;
  }

  public MasterClient createMasterClient() {
    return new MasterClient(
        masterPorts
            .stream()
            .map(port -> new Pair<>("localhost", port))
            .collect(Collectors.toList()));
  }

  public void shutDownMaster(int masterId) {
    masters.get(masterId).close();
  }

  public void startMaster(int masterId) {
    Master master =
        new Master("localhost:21811,localhost:21812,localhost:21813", masterPorts.get(masterId));
    new Thread(master).start();
    masters.set(masterId, master);
  }

  public int getnMasters() {
    return nMasters;
  }

  public void cleanUp() {
    for (int i = 0; i < nMasters; i++) {
      shutDownMaster(i);
    }
  }
}
