# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# Kafka Connect worker for ledger-lakehouse.
#   Debezium Postgres source (from base image)
# + Apache Iceberg Kafka Connect sink (built from the ASF source release)
# + Confluent Avro converter (resolved with Maven)
#
# Build stages are discarded; only the COPY --from results reach the runtime.
# The Gradle stage is cached: it only re-runs when ICEBERG_VERSION changes.
# -----------------------------------------------------------------------------
ARG DEBEZIUM_VERSION=3.7.0.Final
ARG ICEBERG_VERSION=1.12.0
# Published at https://archive.apache.org/dist/iceberg/apache-iceberg-${ICEBERG_VERSION}/
ARG ICEBERG_SHA512=505b4d92baf9a2b55c3893bfa1b50289a0bf0051109d81f0a92825f406c28be9096be31d5c22ed68a2d7abd39fcfb0888729bb1917216b06ac200ac0fbf11a28
# Confluent Platform 8.x pairs with Apache Kafka 4.x (the Debezium image ships Kafka 4.3).
ARG CONFLUENT_VERSION=8.3.2

# ---- Stage 1: build the Iceberg Kafka Connect runtime distribution ----------
FROM eclipse-temurin:21.0.13_9-jdk AS iceberg-build
ARG ICEBERG_VERSION
ARG ICEBERG_SHA512
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
RUN curl -fsSL -o iceberg.tar.gz \
      "https://archive.apache.org/dist/iceberg/apache-iceberg-${ICEBERG_VERSION}/apache-iceberg-${ICEBERG_VERSION}.tar.gz" \
    && echo "${ICEBERG_SHA512}  iceberg.tar.gz" | sha512sum -c - \
    && tar xzf iceberg.tar.gz --strip-components=1 \
    && rm iceberg.tar.gz
# Build only the runtime distribution; skip tests (they need Docker-in-Docker).
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon -x test -x integrationTest \
      :iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip \
    && mkdir -p /out \
    && unzip -q kafka-connect/kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-${ICEBERG_VERSION}.zip -d /out \
    && mv /out/iceberg-kafka-connect-runtime-${ICEBERG_VERSION} /out/iceberg-kafka-connect

# ---- Stage 2: resolve the Confluent Avro converter + transitive deps -------
FROM maven:3.9.11-eclipse-temurin-21 AS avro-deps
ARG CONFLUENT_VERSION
WORKDIR /build
COPY avro-converter-pom.xml pom.xml
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -B dependency:copy-dependencies \
      -Dconfluent.version=${CONFLUENT_VERSION} \
      -DincludeScope=runtime \
      -DoutputDirectory=/out/confluent-avro-converter

# ---- Stage 3: runtime --------------------------------------------------------
FROM quay.io/debezium/connect:${DEBEZIUM_VERSION}
ARG ICEBERG_VERSION
ARG CONFLUENT_VERSION
LABEL org.opencontainers.image.title="ledger-lakehouse-connect" \
      ledger-lakehouse.iceberg-version="${ICEBERG_VERSION}" \
      ledger-lakehouse.confluent-avro-version="${CONFLUENT_VERSION}"
# Each subdirectory of /kafka/connect is an isolated plugin (own classloader).
COPY --from=iceberg-build /out/iceberg-kafka-connect /kafka/connect/iceberg-kafka-connect
COPY --from=avro-deps     /out/confluent-avro-converter /kafka/connect/confluent-avro-converter
