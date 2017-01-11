FROM hysds/pge-base:latest-v2

MAINTAINER mstarch "Michael.D.Starch@jpl.nasa.gov"
LABEL description="Job Iterator Internal Job"

ARG id
ARG gid
#Change UID and GID from build
USER root
RUN set -ex && \
  usermod -u ${id} ops && \
  groupmod -g ${gid} ops && \
  chown -R ops:ops /home/ops/ /data && \
  usermod -g ${gid} ops
USER ops

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data

# provision lightweight-jobs PGE
COPY . /home/ops/verdi/ops/lightweight-jobs


# set entrypoint
USER ops
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
