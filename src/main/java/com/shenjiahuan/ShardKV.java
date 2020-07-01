package com.shenjiahuan;

import com.shenjiahuan.node.Master;
import com.shenjiahuan.node.MasterClient;
import com.shenjiahuan.node.ShardClient;
import com.shenjiahuan.node.ShardServer;
import com.shenjiahuan.util.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ShardKV {
  public static void main(String[] args) {
    final ArgumentParser parser =
        ArgumentParsers.newFor("ShardKV")
            .build()
            .defaultHelp(true)
            .description("A sharded key-value store");

    parser
        .addArgument("-t", "--type")
        .choices("master", "shardserver", "masterclient", "shardclient")
        .required(true);

    parser.addArgument("-p", "--port").type(Integer.class).choices(Arguments.range(0, 65535));

    parser.addArgument("-zk", "--zookeeper");

    parser.addArgument("-g", "--gid").type(Integer.class);

    parser.addArgument("-m", "--masters");

    parser.addArgument("-s", "--shardservers");

    parser.addArgument("-a", "--action").choices("join", "leave");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(1);
    }

    final String type = ns.getString("type");
    final Integer port = ns.getInt("port");
    final String zkUrl = ns.getString("zookeeper");
    final Integer gid = ns.getInt("gid");
    final String masters = ns.getString("masters");
    final String shardServers = ns.getString("shardservers");
    final String action = ns.getString("action");

    List<Pair<String, Integer>> masterList = parseServers(masters);
    List<Pair<String, Integer>> shardServerList = parseServers(shardServers);

    switch (type) {
      case "master":
        {
          handleMaster(port, zkUrl);
          break;
        }
      case "shardserver":
        {
          handleShardServer(port, zkUrl, gid, masterList);
          break;
        }
      case "masterclient":
        {
          handleMasterClient(gid, action, masterList, shardServerList);
          break;
        }
      case "shardclient":
        {
          handleShardClient(masterList);
          break;
        }
    }
  }

  private static void handleMasterClient(
      Integer gid,
      String action,
      List<Pair<String, Integer>> masterList,
      List<Pair<String, Integer>> shardServerList) {
    if (masterList == null || action == null) {
      System.out.println("Invalid arguments");
      System.exit(1);
    }
    MasterClient client = new MasterClient(masterList);
    switch (action) {
      case "join":
        {
          if (gid == null || shardServerList == null) {
            System.out.println("Invalid arguments");
            System.exit(1);
          }
          client.join(gid, shardServerList);
          break;
        }
      case "leave":
        {
          if (gid == null) {
            System.out.println("Invalid arguments");
            System.exit(1);
          }
          client.leave(gid);
          break;
        }
    }
  }

  private static void handleShardClient(List<Pair<String, Integer>> masterList) {
    if (masterList == null) {
      System.out.println("Invalid arguments");
      System.exit(1);
    }
    ShardClient client = new ShardClient(masterList);
    client.interactive();
  }

  private static void handleShardServer(
      Integer port, String zkUrl, Integer gid, List<Pair<String, Integer>> masterList) {
    if (masterList == null || zkUrl == null || port == null || gid == null) {
      System.out.println("Invalid arguments");
      System.exit(1);
    }
    new ShardServer(zkUrl, port, gid, masterList).run();
  }

  private static void handleMaster(Integer port, String zkUrl) {
    if (port == null || zkUrl == null) {
      System.out.println("Invalid arguments");
      System.exit(1);
    }
    new Master(zkUrl, port).run();
  }

  private static List<Pair<String, Integer>> parseServers(String servers) {
    return servers == null
        ? null
        : Arrays.stream(servers.split(";"))
            .map(
                server -> {
                  final String[] tmp = server.split(":");
                  return new Pair<>(tmp[0], Integer.valueOf(tmp[1]));
                })
            .collect(Collectors.toList());
  }
}
