FROM ubuntu:18.04

RUN set -eux \
    && apt update \
    && apt install -y openjdk-8-jre-headless

COPY sharded_kv-1.0-SNAPSHOT.jar /
ENTRYPOINT ["java", "-jar", "sharded_kv-1.0-SNAPSHOT.jar"]
