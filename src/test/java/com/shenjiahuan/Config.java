package com.shenjiahuan;

import com.shenjiahuan.node.Master;
import com.shenjiahuan.node.MasterClient;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Config {

  private int nMasters;
  private List<Master> masters;
  private List<Integer> ports;

  public Config(int nMasters, List<Integer> ports) {
    assert ports.size() == nMasters;

    this.nMasters = nMasters;
    this.masters = new ArrayList<>();
    for (int i = 0; i < nMasters; i++) {
      Master master = new Master("localhost:21811", ports.get(i));
      new Thread(master).start();
      masters.add(master);
    }
    this.ports = ports;
  }

  public MasterClient createClient() {
    return new MasterClient(
        ports.stream().map(port -> new Pair<>("localhost", port)).collect(Collectors.toList()));
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
