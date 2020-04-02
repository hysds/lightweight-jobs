FROM hysds/pge-base:develop

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

# provision lightweight-jobs PGE
USER ops
COPY . /home/ops/lightweight-jobs

RUN sudo mv /home/ops/verdi/ops/lightweight-jobs /home/ops/verdi/ops/lightweight-jobs.orig

RUN unlink /home/ops/verdi/ops/hysds_commons
RUN sudo mv /home/ops/verdi/ops/hysds_commons-0.2.4 /home/ops/verdi/ops/hysds_commons-0.2.4.orig
RUN ln -s /home/ops/verdi/ops/hysds_commons /home/ops/verdi/ops/hysds_commons-0.2.4

RUN sudo mv /home/ops/lightweight-jobs/hysds_commons /home/ops/verdi/ops/

RUN ~/verdi/bin/pip install 'elasticsearch>=7.0.0,<8.0.0'

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops

CMD ["/bin/bash", "--login"]
