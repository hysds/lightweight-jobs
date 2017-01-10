FROM hysds/centos:7

ARG id
ARG gid

# create ops user/group syncing uid and gid with host
RUN set -ex \
 && groupadd -r ops -g ${gid} \
 && useradd -u ${id} -r -g ops -d /home/ops -s /bin/bash -m ops

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
