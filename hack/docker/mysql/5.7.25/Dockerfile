FROM mariadb:5.7.25

COPY on-start.sh /
COPY peer-finder /usr/local/bin/

RUN chmod +x /on-start.sh

# For standalone mariadb
# default entrypoint of parent mariadb:5.7.25
# ENTRYPOINT ["docker-entrypoint.sh"]

# For mariadb group replication
# ENTRYPOINT ["peer-finder"]
