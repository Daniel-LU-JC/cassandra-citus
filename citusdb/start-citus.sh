#!/bin/bash

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <comma-separated-hostnames> <timestamp>"
    exit 1
fi

my_hostname=$(hostname)
my_ip=$(nslookup $my_hostname | awk -F': ' '/Address: / {print $2}')

echo "Checking if psycopg2 binary library exist, if not exist install"
if pip show psycopg2-binary &> /dev/null; then
    echo "psycopg2-binary is installed."
else
    echo "psycopg2-binary is not installed. Installing"
    pip install psycopg2-binary
fi

# Define variables
PG_CTL_COMMAND="/home/stuproj/cs4224o/citusdb/pgsql/bin/pg_ctl"
CREATEDB_COMMAND="/home/stuproj/cs4224o/citusdb/pgsql/bin/createdb"
PSQL_COMMAND="/home/stuproj/cs4224o/citusdb/pgsql/bin/psql"
DATA_DIR="/temp/teamo-data"
LOG_FILE="/temp/teamo-data/log/logfile"

rm -rf $DATA_DIR
$HOME/project_files/scripts/init-citus-db.sh
mkdir -p /temp/teamo-data/log

sed -i 's/^#listen_addresses = .*/listen_addresses = '\''*'\''     # what IP address(es) to listen on;/' $DATA_DIR/postgresql.conf
echo "host    all             all             192.168.0.0/16          trust"| tee -a $DATA_DIR/pg_hba.conf

# Get the list of hostnames from the first argument
hostnames=$1
IFS=',' read -ra hosts <<< "$hostnames"
is_master=false

if [[ ${hosts[0]} =~ \[([0-9]+)-([0-9]+)\] ]]; then
    prefix="${hosts[0]%%\[*}"
    start="${BASH_REMATCH[1]}"
    coordinator_ip=$prefix$start
    if [[ "$(hostname)" == $prefix$start ]]; then
        is_master=true
    fi
else
    coordinator_ip=$hosts[0]
    if [[ "$hostname" == ${hosts[0]} ]]; then
        is_master=true
    fi
fi

# Start PostgreSQL server
$PG_CTL_COMMAND -D "$DATA_DIR" -l "$LOG_FILE" start
$CREATEDB_COMMAND
$PSQL_COMMAND -c "CREATE EXTENSION citus;"

# 1. Add worker nodes on master 2. Add hosts into a list
if $is_master; then
    sleep 15
    echo "$(hostname) is master"
    for host in "${hosts[@]}"; do
        if [[ $host =~ \[([0-9]+)-([0-9]+)\] ]]; then
            prefix="${host%%\[*}"
            start="${BASH_REMATCH[1]}"
            end="${BASH_REMATCH[2]}"
            
            # Expanded contracted hostnames
            for ((i=start; i<=end; i++)); do
                # Add worker to pg_worker_list.conf
                if [[ $(hostname) != $prefix$i ]]; then
                    echo "$prefix$i 5111"
                    echo "$prefix$i 5111" | tee -a $DATA_DIR/pg_worker_list.conf
                    $PSQL_COMMAND -c "SELECT * from citus_add_node('$prefix$i', 5111);"
                fi
                
                # Add host to host_list
                host_list+=($prefix$i)
            done
        else
            # Add worker to pg_worker_list.conf
            if [[ $(hostname) != $prefix$i ]]; then
                echo "$host 5111"
                echo "$host 5111" | tee -a $DATA_DIR/pg_worker_list.conf
            fi

            # Add host to host_list
            host_list+=($host)
        fi
    done

    # Restart master node
    $PG_CTL_COMMAND -D "$DATA_DIR" -l "$LOG_FILE" restart
else
    for host in "${hosts[@]}"; do
        if [[ $host =~ \[([0-9]+)-([0-9]+)\] ]]; then
            prefix="${host%%\[*}"
            start="${BASH_REMATCH[1]}"
            end="${BASH_REMATCH[2]}"
            
            # Expanded contracted hostnames
            for ((i=start; i<=end; i++)); do
                # Add host to host_list
                host_list+=($prefix$i)
            done
        else
            # Add host to host_list
            host_list+=($host)
        fi
    done
fi

# # Tail logs
# tail -400f $LOG_FILE

# Find the index of the target host
index=-1
for i in "${!host_list[@]}"; do
    if [[ "${host_list[i]}" == "$(hostname)" ]]; then
        index=$i
        break
    fi
done

sleep 60

echo $(hostname): sleep done

# Create output directory
output_dir="$HOME/output/citusdb/$2"
mkdir -p $output_dir

while true; do
    file="$output_dir/create_table.out"
    if [ -e "$file" ]; then
        echo "File '$file' exists."
        break
    else
        echo "$my_ip - File '$file' does not exist. Waiting for 10 seconds..."
        sleep 10
    fi
done

# if [[ $index == 0 ]]; then
#     python $HOME/citusdb/load_data.py "$(($index+1))" $coordinator_ip
#     python $HOME/citusdb/load_data.py "$(($index+2))" $coordinator_ip
# fi
# python $HOME/citusdb/load_data.py "$(($index+3))" $coordinator_ip
# echo "load $(($index+3)) done" > $output_dir/$(($index+3))_load.out

# i=3
# while [ $i -lt 8 ]; do
#     file="$output_dir/${i}_load.out"
#     if [ -e "$file" ]; then
#         echo "File '$file' exists."
#         ((i++))
#     else
#         echo "$my_ip - File '$file' does not exist. Waiting for 60 seconds..."
#         sleep 60
#     fi
# done

# Find the clients with the same modulo 5 result
for i in $(seq 0 19); do
    if [ $((i % 5)) -eq $index ]; then
        client_list+=($i)
    fi
done

echo "$(hostname) - client list: ${index}"
echo "$(hostname) - client list: ${client_list[@]}"

# Run the 4 clients concurrently
for i in "${client_list[@]}"; do
    echo "$my_ip: starting client $i"
    python $HOME/cassandra/client.py < "$HOME/project_files/xact_files/$i.txt" 2> "$output_dir/$i.out" &
    echo "$my_ip: output to $output_dir/$i.out"
done

# synchronize till the end of all the execution
finish_file="$output_dir/finish.out"
while true; do
    if [ -e "$finish_file" ]; then
        break
    else
        sleep 30
    fi
done

echo $my_ip: done
