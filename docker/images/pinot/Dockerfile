#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
ARG JAVA_VERSION=21
FROM --platform=$BUILDPLATFORM apachepinot/pinot-base-build:${JAVA_VERSION}-amazoncorretto AS pinot_build_env

LABEL MAINTAINER=dev@pinot.apache.org

ARG PINOT_BRANCH=master
ARG JAVA_VERSION=21
ARG PINOT_GIT_URL="https://github.com/apache/pinot.git"
ARG CI=true

RUN echo "Build Pinot based on image: apachepinot/pinot-base-build:${JAVA_VERSION}-amazoncorretto"
RUN echo "Current build system CPU arch is [ $(uname -m) ]"

RUN echo "Trying to build Pinot from [ ${PINOT_GIT_URL} ] on branch [ ${PINOT_BRANCH} ] and CI [ ${CI} ]"
ENV PINOT_HOME=/opt/pinot
ENV PINOT_BUILD_DIR=/opt/pinot-build
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG /opt/.m2

RUN git clone ${PINOT_GIT_URL} ${PINOT_BUILD_DIR} && \
    cd ${PINOT_BUILD_DIR} && \
    git checkout ${PINOT_BRANCH} && \
    mvn install package -DskipTests -Pbin-dist -Pbuild-shaded-jar -Djdk.version=${JAVA_VERSION} -T1C && \
    rm -rf /root/.m2 && \
    mkdir -p ${PINOT_HOME}/configs && \
    mkdir -p ${PINOT_HOME}/data && \
    cp -r build/* ${PINOT_HOME}/. && \
    chmod +x ${PINOT_HOME}/bin/*.sh

# Same runtime as apachepinot/pinot-base-runtime:21-amazoncorretto before upstream rebased that tag on Debian (#18178)
FROM amazoncorretto:21-al2023-jdk@sha256:70c4c569bf3f794e4b9f22619efb64a072a5d97fd66867fd134398c12d6f2905

RUN yum update -y && \
  yum swap -y curl-minimal curl && \
  yum install -y procps less wget git-core sysstat libtasn1 zstd tar gzip && \
  yum clean all && \
  rm -rf /var/cache/yum

RUN case `uname -m` in \
  x86_64) arch=x64; ;; \
  aarch64) arch=arm64; ;; \
  *) echo "platform=$(uname -m) un-supported, exit ..."; exit 1; ;; \
  esac \
  && mkdir -p /usr/local/lib/async-profiler \
  && curl -L https://github.com/async-profiler/async-profiler/releases/download/v4.3/async-profiler-4.3-linux-${arch}.tar.gz | tar -xz --strip-components 1 -C /usr/local/lib/async-profiler \
  && ln -s /usr/local/lib/async-profiler/bin/asprof /usr/local/bin/async-profiler

LABEL MAINTAINER=dev@pinot.apache.org

ENV PINOT_HOME=/opt/pinot
ENV JAVA_OPTS="-Xms4G -Xmx4G -Dpinot.admin.system.exit=false"

VOLUME ["${PINOT_HOME}/configs", "${PINOT_HOME}/data"]

COPY --from=pinot_build_env ${PINOT_HOME} ${PINOT_HOME}
COPY bin ${PINOT_HOME}/bin
COPY etc ${PINOT_HOME}/etc
COPY examples ${PINOT_HOME}/examples

RUN wget -O ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent-1.5.0.jar https://github.com/prometheus/jmx_exporter/releases/download/1.5.0/jmx_prometheus_javaagent-1.5.0.jar && \
    ln -s ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent-1.5.0.jar ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent.jar

# expose ports for controller/broker/server/admin
EXPOSE 9000 8099 8098 8097 8096

WORKDIR ${PINOT_HOME}

ENTRYPOINT ["./bin/pinot-admin.sh"]

CMD ["-help"]
