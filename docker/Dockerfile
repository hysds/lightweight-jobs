FROM hysds/pge-base:latest

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

# provision lightweight-jobs PGE
COPY . /home/ops/lightweight-jobs

USER ops
RUN mv /home/ops/lightweight-jobs/hysds_commons /home/ops/verdi/ops/
RUN ~/verdi/bin/pip install 'elasticsearch>=7.0.0,<8.0.0'

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
