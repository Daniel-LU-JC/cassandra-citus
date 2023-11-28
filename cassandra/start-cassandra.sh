#!/bin/bash

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <timestamp>"
    exit 1
fi

timestamp=$1

export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64

my_hostname=$(hostname)
my_ip=$(nslookup $my_hostname | awk -F': ' '/Address: / {print $2}')
#my_ip=$(nslookup $my_hostname | grep 192 | awk '{gsub(/^[ \t]+|[ \t]+$/, ""); print $2}')
echo $my_ip

export CASSANDRA_HOME=/temp/cs4224o/apache-cassandra-4.1.3
rm -rf /temp/cs4224o
mkdir -p /temp/cs4224o
cp -r $HOME/cassandra/apache-cassandra-4.1.3 $CASSANDRA_HOME
mkdir -p $CASSANDRA_HOME/logs
output_dir=$HOME/output/cassandra/$timestamp

sed -e "s/@@MY_IP@@/$my_ip/g" cassandra.yaml > $CASSANDRA_HOME/conf/cassandra-$my_ip.yaml

list=$(cat $CASSANDRA_HOME/conf/cassandra-$my_ip.yaml | grep -A9 seed_provider | tail -n +10 | awk -F': ' '/- seeds: / {print $2}')

# Convert the comma-separated string to an array
IFS=',' read -ra ips <<< "$list"

# Find the index of the target IP
index=-1
for i in "${!ips[@]}"; do
    if [[ "${ips[i]}" == "$my_ip" ]]; then
        index=$i
        break
    fi
done

cd $CASSANDRA_HOME

bin/cassandra -Dcassandra.ignore_dc=true -Dcassandra.config=file://$CASSANDRA_HOME/conf/cassandra-$my_ip.yaml -f > $CASSANDRA_HOME/logs/cassandra.log 2>&1 &

echo $my_ip: started cassandra

sleep 120

if [[ $index == 0 ]]; then
    python $HOME/junchen/cs5424-project/cassandra/dataload/load_data_"$(($index+1))".py
    python $HOME/junchen/cs5424-project/cassandra/dataload/load_data_"$(($index+2))".py
fi
python $HOME/junchen/cs5424-project/cassandra/dataload/load_data_"$(($index+3))".py
echo "load $(($index+3)) done" > $output_dir/$(($index+3))_load.out

i=3
while [ $i -lt 8 ]; do
    file="$output_dir/${i}_load.out"
    if [ -e "$file" ]; then
        echo "File '$file' exists."
        ((i++))
    else
        echo "$my_ip - File '$file' does not exist. Waiting for 10 seconds..."
        sleep 10
    fi
done

# Find the clients with the same modulo 5 result
for i in $(seq 0 19); do
    if [ $((i % 5)) -eq $index ]; then
        client_list+=($i)
    fi
done

echo "$my_ip - client list: ${client_list[@]}"

# Create output directory
# output_dir="$HOME/output/cassandra/$1"
# mkdir -p $output_dir

# Run the 4 clients concurrently
for i in "${client_list[@]}"; do
    echo "$my_ip: starting client $i"
    python $HOME/junchen/cs5424-project/cassandra/client.py < "$HOME/project_files/xact_files/$i.txt" 2> "$output_dir/$i.out" &
    echo "$my_ip: output to $output_dir/$i.out"
done

# synchronize till the end of all the execution
finish_file="$output_dir/finish.out"
while true; do
    if [ -e "$finish_file" ]; then
        break
    else
        echo $my_ip: hanging...
        sleep 60
    fi
done

echo $my_ip: done

