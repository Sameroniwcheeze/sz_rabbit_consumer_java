# docker build -t brian/sz_simple_redoer_java .
# docker run --user $UID -it -v $PWD:/data -e SENZING_ENGINE_CONFIGURATION_JSON brian/sz_rabbit_consumer_java

ARG BASE_IMAGE=senzing/senzingapi-runtime:latest

FROM ${BASE_IMAGE}

ENV REFRESHED_AT=2023-06-30

LABEL Name="sameroni/sz_rabbit_consumer_java" \
      Maintainer="brianmacy@gmail.com" \
      Version="DEV"


# Run as "root" for system installation.

USER root

ENV PATH = ${PATH}:/apache-maven-${MAVEN_VERSION}/bin

COPY g2redoer /build
WORKDIR /build

RUN apt-get update \
 && apt-get -y install postgresql-client \
 && apt-get -y install openjdk-11-jre-headless maven \
 && apt-get -y clean \
 && ls \
 && mvn clean install \
 && mkdir /app \
 && cp target/consumer-1.0.0-SNAPSHOT.jar /app/ \
 && cd / \
 && rm -rf /build \
 && apt-get -y remove maven \
 && apt-get -y autoremove \
 && apt-get -y clean

WORKDIR /app
CMD ["java", "-jar", "consumer-1.0.0-SNAPSHOT.jar"]

