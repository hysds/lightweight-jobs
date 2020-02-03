FROM hysds/pge-base:latest

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

COPY . /home/ops/lightweight-jobs
RUN mv /home/ops/verdi/ops/hysds_commons /home/ops/verdi/ops/hysds_commons.orig
RUN mv /home/ops/lightweight-jobs/hysds_commons /home/ops/verdi/ops/

# provision lightweight-jobs PGE
USER ops
RUN ~/verdi/bin/pip install 'elasticsearch>=7.0.0,<8.0.0'

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
