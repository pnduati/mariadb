FROM mariadb:8.0.14

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    netcat \
  && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man /tmp/*

COPY osm /usr/local/bin/osm
COPY mariadb-tools.sh /usr/local/bin/mariadb-tools.sh

ENTRYPOINT ["mariadb-tools.sh"]
