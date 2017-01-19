FROM hysds/pge-base:latest-v2

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

ARG id
ARG gid
#Change UID and GID from build
USER root
RUN set -ex && \
  usermod -u ${id} ops && \
  groupmod -g ${gid} ops && \
  chown -R ops:ops /home/ops/ /data && \
  usermod -g ${gid} ops

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data

RUN yum install -y sendmail && echo "127.0.0.1 localhost localhost.localdomain" >> /etc/hosts
# provision lightweight-jobs PGE
USER ops
COPY . /home/ops/verdi/ops/lightweight-jobs

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
