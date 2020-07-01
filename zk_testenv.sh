#!/bin/bash

set -x

ZOO1_IP=10.0.1.1
ZOO2_IP=10.0.1.2
ZOO3_IP=10.0.1.3

ZK_SERVER="${ZOO1_IP},${ZOO2_IP},${ZOO3_IP}"

docker rm -f zoo1 > /dev/null 2>&1
docker rm -f zoo2 > /dev/null 2>&1
docker rm -f zoo3 > /dev/null 2>&1

docker network rm kv

docker network create \
--subnet=10.0.0.0/16 \
--gateway=10.0.0.1 \
kv


docker run --rm --name zoo1 --hostname zoo1 --network kv --ip ${ZOO1_IP} -e ZOO_MY_ID=1 -e ZOO_SERVERS="server.1=0.0.0.0:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=zoo3:2888:3888;2181" -p 21811:2181 -d shenjiahuan:zookeeper
docker run --rm --name zoo2 --hostname zoo2 --network kv --ip ${ZOO2_IP} -e ZOO_MY_ID=2 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=0.0.0.0:2888:3888;2181 server.3=zoo3:2888:3888;2181" -p 21812:2181 -d shenjiahuan:zookeeper
docker run --rm --name zoo3 --hostname zoo3 --network kv --ip ${ZOO3_IP} -e ZOO_MY_ID=3 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=0.0.0.0:2888:3888;2181" -p 21813:2181 -d shenjiahuan:zookeeper
