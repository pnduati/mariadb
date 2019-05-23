#!/usr/bin/env bash

#set -eoux pipefail

# Environment variables passed from Pod env are as follows:
#
#   GROUP_NAME          = a uuid treated as the name of the replication group
#   BASE_NAME           = name of the StatefulSet (same as the name of CRD)
#   BASE_SERVER_ID      = server-id of the primary member
#   GOV_SVC             = the name of the governing service
#   POD_NAMESPACE       = the Pods' namespace
#   MARIADB_ROOT_USERNAME = root user name
#   MARIADB_ROOT_PASSWORD = root password

script_name=${0##*/}
NAMESPACE="$POD_NAMESPACE"
USER="$MARIADB_ROOT_USERNAME"
PASSWORD="$MARIADB_ROOT_PASSWORD"

function timestamp() {
  date +"%Y/%m/%d %T"
}

function log() {
  local type="$1"
  local msg="$2"
  echo "$(timestamp) [$script_name] [$type] $msg"
}

# get_host_name() expects only one argument and that is the index of the Pod of StatefulSet.
# And it forms the FQDN (Fully Qualified Domain Name) of the $1'th Pod of StatefulSet.
function get_host_name() {
#  echo -n "$BASE_NAME-$1.$GOV_SVC.$NAMESPACE.svc.cluster.local"
  echo -n "$BASE_NAME-$1.$GOV_SVC.$NAMESPACE"
}

# get the host names from stdin sent by peer-finder program
cur_hostname=$(hostname)
export cur_host=
log "INFO" "Reading standard input..."
while read -ra line; do
  if [[ "${line}" == *"${cur_hostname}"* ]]; then
#    cur_host="$line"
    cur_host=$(echo -n ${line} | sed -e "s/.svc.cluster.local//g")
    log "INFO" "I am $cur_host"
  fi
#  peers=("${peers[@]}" "$line")
  tmp=$(echo -n ${line} | sed -e "s/.svc.cluster.local//g")
  peers=("${peers[@]}" "$tmp")

done
log "INFO" "Trying to start group with peers'${peers[*]}'"

# store the value for the variables those will be written in /etc/mariadb/my.cnf file

# comma separated host names
export hosts=$(echo -n ${peers[*]} | sed -e "s/ /,/g")

# comma separated seed addresses of the hosts (host1:port1,host2:port2,...)
export seeds=$(echo -n ${hosts} | sed -e "s/,/:33060,/g" && echo -n ":33060")

# server-id calculated as $BASE_SERVER_ID + pod index
declare -i srv_id=$(hostname | sed -e "s/${BASE_NAME}-//g")
((srv_id += $BASE_SERVER_ID))

export cur_addr="${cur_host}:33060"

# Get ip_whitelist
# https://dev.mariadb.com/doc/refman/5.7/en/group-replication-options.html#sysvar_group_replication_ip_whitelist
# https://dev.mariadb.com/doc/refman/5.7/en/group-replication-ip-address-whitelisting.html
#
# Command $(hostname -I) returns a space separated IP list. We need only the first one.
myips=$(hostname -I)
first=${myips%% *}
# Now use this IP with CIDR notation
export whitelist="${first}/16"

# the mariadbd configurations have take by following
# 01. official doc: https://dev.mariadb.com/doc/refman/5.7/en/group-replication-configuring-instances.html
# 02. digitalocean doc: https://www.digitalocean.com/community/tutorials/how-to-configure-mariadb-group-replication-on-ubuntu-16-04
log "INFO" "Storing default mariadbd config into /etc/mariadb/my.cnf"
cat >>/etc/mariadb/my.cnf <<EOL

[mariadbd]

# General replication settings
gtid_mode = ON
enforce_gtid_consistency = ON
master_info_repository = TABLE
relay_log_info_repository = TABLE
binlog_checksum = NONE
log_slave_updates = ON
log_bin = binlog
binlog_format = ROW
transaction_write_set_extraction = XXHASH64
loose-group_replication_bootstrap_group = OFF
loose-group_replication_start_on_boot = OFF
loose-group_replication_ssl_mode = REQUIRED
loose-group_replication_recovery_use_ssl = 1

# Shared replication group configuration
loose-group_replication_group_name = "${GROUP_NAME}"
loose-group_replication_ip_whitelist = "${whitelist}"
loose-group_replication_group_seeds = "${seeds}"

# Single or Multi-primary mode? Uncomment these two lines
# for multi-primary mode, where any host can accept writes
#loose-group_replication_single_primary_mode = OFF
#loose-group_replication_enforce_update_everywhere_checks = ON

# Host specific replication configuration
server_id = ${srv_id}
#bind-address = "${cur_host}"
bind-address = "0.0.0.0"
report_host = "${cur_host}"
loose-group_replication_local_address = "${cur_addr}"
EOL

log "INFO" "Starting mariadb server with 'docker-entrypoint.sh mariadbd $@'..."

# ensure the mariadbd process be stopped
/etc/init.d/mariadb stop

# run the mariadbd process in background with user provided arguments if any
docker-entrypoint.sh mariadbd $@ &
pid=$!
log "INFO" "The process id of mariadbd is '$pid'"

# wait for all mariadb servers be running (alive)
for host in ${peers[*]}; do
  for i in {900..0}; do
    out=$(mariadbadmin -u ${USER} --password=${PASSWORD} --host=${host} ping 2>/dev/null)
    if [[ "$out" == "mariadbd is alive" ]]; then
      break
    fi

    echo -n .
    sleep 1
  done

  if [[ "$i" == "0" ]]; then
    echo ""
    log "ERROR" "Server ${host} start failed..."
    exit 1
  fi
done

log "INFO" "All servers (${peers[*]}) are ready"

# now we need to configure a replication user for each server.
# the procedures for this have been taken by following
# 01. official doc (section from 17.2.1.3 to 17.2.1.5): https://dev.mariadb.com/doc/refman/5.7/en/group-replication-user-credentials.html
# 02. digitalocean doc: https://www.digitalocean.com/community/tutorials/how-to-configure-mariadb-group-replication-on-ubuntu-16-04
#####################################################################
# Begin initialization process                                      #
#####################################################################
export mariadb_header="mariadb -u ${USER}"

# this is to bypass the warning message for using password
export MARIADB_PWD=${PASSWORD}
export member_hosts=($(echo -n ${hosts} | sed -e "s/,/ /g"))

for host in ${member_hosts[*]}; do
  log "INFO" "Initializing the server (${host})..."

  mariadb="$mariadb_header --host=$host"

  out=$(${mariadb} -N -e "select count(host) from mariadb.user where mariadb.user.user='repl';" | awk '{print$1}')
  if [[ "$out" -eq "0" ]]; then

    # is_new is an array,
    #                              | 1; if i'th host is created for the 1st time
    #       where ${is_new[$i]} =  |
    #                              | 0; otherwise (may rebooted)
    # So, for the first time creation a '1' otherwise a '0' will be appended
    is_new=("${is_new[@]}" "1")

    log "INFO" "Replication user not found and creating one..."
    ${mariadb} -N -e "SET SQL_LOG_BIN=0;"
    ${mariadb} -N -e "CREATE USER 'repl'@'%' IDENTIFIED BY 'password' REQUIRE SSL;"
    ${mariadb} -N -e "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';"
    ${mariadb} -N -e "FLUSH PRIVILEGES;"
    ${mariadb} -N -e "SET SQL_LOG_BIN=1;"

    ${mariadb} -N -e "CHANGE MASTER TO MASTER_USER='repl', MASTER_PASSWORD='password' FOR CHANNEL 'group_replication_recovery';"
  else
    log "INFO" "Replication user info exists"
    is_new=("${is_new[@]}" "0")
  fi

  # ensure the group replication plugin be installed
  out=$(${mariadb} -N -e 'SHOW PLUGINS;' | grep group_replication)
  if [[ -z "$out" ]]; then
    log "INFO" "Installing group replication plugin..."
    ${mariadb} -e "INSTALL PLUGIN group_replication SONAME 'group_replication.so';"
  else
    log "INFO" "Already group replication plugin is installed"
  fi
done
#####################################################################
# End initialization process                                        #
#####################################################################

function find_group() {
  # TODO: Need to handle for multiple group existence
  group_found=0
  for host in $@; do

    export mariadb="$mariadb_header --host=${host}"
    # value may be 'UNDEFINED'
    primary_id=$(${mariadb} -N -e "SHOW STATUS WHERE Variable_name = 'group_replication_primary_member';" | awk '{print $2}')
    if [[ -n "$primary_id" ]]; then
      ids=($(${mariadb} -N -e "SELECT MEMBER_ID FROM performance_schema.replication_group_members WHERE MEMBER_STATE = 'ONLINE' OR MEMBER_STATE = 'RECOVERING';"))

      for id in ${ids[@]}; do
        if [[ "${primary_id}" == "${id}" ]]; then
          group_found=1
          primary_host=$(${mariadb} -N -e "SELECT MEMBER_HOST FROM performance_schema.replication_group_members WHERE MEMBER_ID = '${primary_id}';" | awk '{print $1}')

          break
        fi
      done
    fi

    if [[ "$group_found" == "1" ]]; then
      break
    fi

  done

  echo -n "${group_found}"
}

log "INFO" "Checking whether there exists any replication group or not..."
export primary_host=$(get_host_name 0)
find_group ${member_hosts[*]}
export found=$(find_group ${member_hosts[*]})

# filter the Pod index from the variable $primary_host
#primary_idx=$(echo ${primary_host} | sed -e "s/.${GOV_SVC}.${NAMESPACE}.svc.cluster.local//g" | sed -e "s/${BASE_NAME}-//g")
primary_idx=$(echo ${primary_host} | sed -e "s/.${GOV_SVC}.${NAMESPACE}//g" | sed -e "s/${BASE_NAME}-//g")

# First start group replication inside the primary
#####################################################################
# Begin group replication bootstrap process if no group exists      #
#####################################################################

if [[ "$found" == "0" ]]; then
  mariadb="$mariadb_header --host=$primary_host"

  # get the member state from performance_schema.replication_group_members
  out=$(${mariadb} -N -e "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST = '$primary_host';")
  if [[ -z "$out" || "$out" == "OFFLINE" ]]; then
    log "INFO" "No group is found and bootstrapping one on host '$primary_host'..."

    ${mariadb} -N -e "STOP GROUP_REPLICATION;"

    # reset is needed for the first time creation
    if [[ "${is_new[$primary_idx]}" -eq "1" ]]; then
      log "INFO" "RESET MASTER in primary host $primary_host..."
      ${mariadb} -N -e "RESET MASTER;"
    fi

    ${mariadb} -N -e "SET GLOBAL group_replication_bootstrap_group=ON;"
    ${mariadb} -N -e "START GROUP_REPLICATION;"
    ${mariadb} -N -e "SET GLOBAL group_replication_bootstrap_group=OFF;"

    log "INFO" "A new group (name $GROUP_NAME) is bootstrapped on $primary_host"

  else
    log "INFO" "No group is found and member state is '$out' on host '$primary_host'..."
  fi

else
  log "INFO" "A group is found and the primary host is '$primary_host'..."
fi
#####################################################################
# End bootstrap process                                             #
#####################################################################

# Now start group replication inside the others
#####################################################################
# Begin joining others members to the group                         #
#####################################################################
declare -i host_idx=0

for host in ${member_hosts[*]}; do

  if [[ "$host" != "$primary_host" ]]; then
    mariadb="$mariadb_header --host=$host"

    # get the member state from performance_schema.replication_group_members
    out=$(${mariadb} -N -e "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST = '$host';")

    if [[ -z "$out" || "$out" == "OFFLINE" ]]; then
      log "INFO" "Starting group replication on (${host})..."

      ${mariadb} -N -e "STOP GROUP_REPLICATION;"

      # reset is needed for the first time creation
      if [[ "${is_new[$host_idx]}" -eq "1" ]]; then
        log "INFO" "RESET MASTER in host $host..."
        ${mariadb} -N -e "RESET MASTER;"
      fi

      ${mariadb} -N -e "START GROUP_REPLICATION;"

      log "INFO" "$host is joined the group $GROUP_NAME"

    else
      log "INFO" "Member state is '${out}' on host '${host}'..."
    fi
  fi

  ((host_idx++))
done
#####################################################################
# End joining process                                               #
#####################################################################

# wait for mariadbd process running in background
log "INFO" "Waiting for mariadbd server process running..."
wait $pid
