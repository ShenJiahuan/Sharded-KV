package com.shenjiahuan;

import com.shenjiahuan.node.Master;
import com.shenjiahuan.node.MasterClient;
import com.shenjiahuan.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestMasterConfig {

  private final int nMasters;
  protected final List<Master> masters;
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
    new Thread(masters.get(masterId)).start();
  }

  public int getnMasters() {
    return nMasters;
  }
}
