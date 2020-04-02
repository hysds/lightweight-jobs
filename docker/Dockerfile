FROM hysds/pge-base:develop

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

# provision lightweight-jobs PGE
USER ops
COPY . /home/ops/lightweight-jobs

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops

CMD ["/bin/bash", "--login"]
