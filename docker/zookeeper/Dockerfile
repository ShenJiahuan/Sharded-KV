FROM ubuntu:18.04

ARG ZK_DIST=zookeeper-3.6.1

ENV ZOO_CONF_DIR=/conf \
    ZOO_DATA_DIR=/data \
    ZOO_DATA_LOG_DIR=/datalog \
    ZOO_LOG_DIR=/logs \
    ZOO_TICK_TIME=2000 \
    ZOO_INIT_LIMIT=5 \
    ZOO_SYNC_LIMIT=2 \
    ZOO_AUTOPURGE_PURGEINTERVAL=0 \
    ZOO_AUTOPURGE_SNAPRETAINCOUNT=3 \
    ZOO_MAX_CLIENT_CNXNS=60 \
    ZOO_STANDALONE_ENABLED=true \
    ZOO_ADMINSERVER_ENABLED=true

RUN set -eux \
    && apt update \
    && apt install -y openjdk-8-jre-headless wget \
    && wget https://downloads.apache.org/zookeeper/$ZK_DIST/apache-$ZK_DIST-bin.tar.gz \
    && tar zxvf apache-$ZK_DIST-bin.tar.gz \
    && ln -s apache-$ZK_DIST-bin zookeeper \
    && ln -s zookeeper/conf $ZOO_CONF_DIR

COPY docker-entrypoint.sh /
COPY reset.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/zookeeper/bin/zkServer.sh", "start-foreground"]