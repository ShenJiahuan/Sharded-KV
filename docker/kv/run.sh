#!/bin/bash

set -x

ZOO1_IP=10.0.1.1
ZOO2_IP=10.0.1.2
ZOO3_IP=10.0.1.3

MASTER1_IP=10.0.2.1
MASTER2_IP=10.0.2.2

SHARDSERVER11_IP=10.0.3.1
SHARDSERVER12_IP=10.0.3.2

SHARDSERVER21_IP=10.0.4.1
SHARDSERVER22_IP=10.0.4.2

SHARDSERVER31_IP=10.0.5.1
SHARDSERVER32_IP=10.0.5.2

ZK_SERVER="${ZOO1_IP},${ZOO2_IP},${ZOO3_IP}"

docker rm -f zoo1 > /dev/null 2>&1
docker rm -f zoo2 > /dev/null 2>&1
docker rm -f zoo3 > /dev/null 2>&1

docker rm -f master1 > /dev/null 2>&1
docker rm -f master2 > /dev/null 2>&1

docker rm -f shardserver11 > /dev/null 2>&1
docker rm -f shardserver12 > /dev/null 2>&1

docker rm -f shardserver21 > /dev/null 2>&1
docker rm -f shardserver22 > /dev/null 2>&1

docker rm -f shardserver31 > /dev/null 2>&1
docker rm -f shardserver32 > /dev/null 2>&1

docker network rm kv

docker network create \
--subnet=10.0.0.0/16 \
--gateway=10.0.0.1 \
kv


docker run --rm --name zoo1 --hostname zoo1 --network kv --ip ${ZOO1_IP} -e ZOO_MY_ID=1 -e ZOO_SERVERS="server.1=0.0.0.0:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=zoo3:2888:3888;2181" -d shenjiahuan:zookeeper
docker run --rm --name zoo2 --hostname zoo2 --network kv --ip ${ZOO2_IP} -e ZOO_MY_ID=2 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=0.0.0.0:2888:3888;2181 server.3=zoo3:2888:3888;2181" -d shenjiahuan:zookeeper
docker run --rm --name zoo3 --hostname zoo3 --network kv --ip ${ZOO3_IP} -e ZOO_MY_ID=3 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=0.0.0.0:2888:3888;2181" -d shenjiahuan:zookeeper

docker exec zoo1 /reset.sh

docker run --rm --name master1 --hostname master1 --network kv --ip ${MASTER1_IP} -d -it shenjiahuan:kv -t master -p 2000 -zk zoo1:2181
docker run --rm --name master2 --hostname master2 --network kv --ip ${MASTER2_IP} -d -it shenjiahuan:kv -t master -p 2000 -zk zoo1:2181

docker run --rm --name shardserver11 --hostname shardserver11 --network kv --ip ${SHARDSERVER11_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 0 -m "master1:2000;master2:2000"
docker run --rm --name shardserver12 --hostname shardserver12 --network kv --ip ${SHARDSERVER12_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 0 -m "master1:2000;master2:2000"

docker run --rm --name shardserver21 --hostname shardserver21 --network kv --ip ${SHARDSERVER21_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 1 -m "master1:2000;master2:2000"
docker run --rm --name shardserver22 --hostname shardserver22 --network kv --ip ${SHARDSERVER22_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 1 -m "master1:2000;master2:2000"

docker run --rm --name shardserver31 --hostname shardserver31 --network kv --ip ${SHARDSERVER31_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 2 -m "master1:2000;master2:2000"
docker run --rm --name shardserver32 --hostname shardserver32 --network kv --ip ${SHARDSERVER32_IP} -d -it shenjiahuan:kv -t shardserver -p 10000 -zk zoo1:2181 -g 2 -m "master1:2000;master2:2000"

docker run --rm --network kv -d -it shenjiahuan:kv -t masterclient -a join -g 0 -m "master1:2000;master2:2000" -s "shardserver11:10000;shardserver12:10000"
docker run --rm --network kv -d -it shenjiahuan:kv -t masterclient -a join -g 1 -m "master1:2000;master2:2000" -s "shardserver21:10000;shardserver22:10000"
docker run --rm --network kv -d -it shenjiahuan:kv -t masterclient -a join -g 2 -m "master1:2000;master2:2000" -s "shardserver31:10000;shardserver32:10000"

docker run --rm --network kv -it shenjiahuan:kv -t shardclient -m "master1:2000;master2:2000"
