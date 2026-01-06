FROM hysds/pge-base:develop

MAINTAINER malarout "Namrata.Malarout@jpl.nasa.gov"
LABEL description="Lightweight System Jobs"

# provision lightweight-jobs PGE

# create ops user and group
RUN useradd -M -u 10005 ops

# softlink /home/ops
RUN ln -s /root /home/ops

# Make sure $HOME is set when we run this container
ENV HOME=/home/ops

USER ops
COPY . /home/ops/lightweight-jobs

RUN set -ex \
 && source /home/ops/.bash_profile \
 && cd /root/verdi/ops \
 && rm -rf hysds \
 && git clone -b HC-600 https://github.com/hysds/hysds.git \
 && cd hysds \
 && pip install -e . \
 && ln -s /home/ops/verdi/etc/celeryconfig.py celeryconfig.py \
 && cd ../ \
 && rm -rf pele \
 && git clone -b HC-600 https://github.com/hysds/pele.git \
 && cd pele \
 && pip install -e . \
 && cd ../ \
 && rm -rf hysds_commons \
 && git clone -b HC-600 https://github.com/hysds/hysds_commons.git \
 && cd hysds_commons \
 && pip install -e . \

# set entrypoint
ENTRYPOINT ["/entrypoint-pge-with-stats.sh"]

WORKDIR /home/ops

CMD ["/bin/bash", "--login"]
