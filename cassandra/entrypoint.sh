#!/bin/bash
#SBATCH -n 6
#SBATCH --partition=long
#SBATCH --ntasks-per-node=1
#SBATCH --mem=16G

# Check if at least two arguments should be provided
# if [ $# -lt 2 ]; then
#     echo "Usage: $0 <comma-separated-hostnames> <slurm-jobid>"
#     exit 1
# fi

# Get the list of hostnames from the first argument
hostnames=$SLURM_JOB_NODELIST
IFS=',' read -ra hosts <<< "$hostnames"

# jobid=$2

for host in "${hosts[@]}"; do
    if [[ $host =~ \[([0-9]+)-([0-9]+)\] ]]; then
        prefix="${host%%\[*}"
        start="${BASH_REMATCH[1]}"
        end="${BASH_REMATCH[2]}"
        
        # Expanded contracted hostnames and convert them to IPs
        for ((i=start; i<=end; i++)); do
            ip=$(nslookup "$prefix$i" | awk -F': ' '/Address: / {print $2}')
            if [[ ! -z "$ip" ]]; then
                ips+=("$ip")
            fi
        done
    else
        # Convert hostnames to IPs
        ip=$(nslookup "$host" | awk -F': ' '/Address: / {print $2}')
        if [[ ! -z "$ip" ]]; then
            ips+=("$ip")
        fi
    fi
done

# remove the master node from the ip list
local_hostname=$(hostname)
local_ip=$(hostname -I | awk '{print $1}')
other_ips=()
for ip in "${ips[@]}"; do
    if [[ $ip != $local_ip ]]; then
        other_ips+=("$ip")
    fi
done

# echo ${other_ips[@]}
other_ipstring=$(echo "${other_ips[@]}" | sed 's/ /,/g')
# echo $other_ipstring

sed "s/@@ALL_IPS@@/$other_ipstring/g" cassandra_template.yaml > cassandra.yaml

timestamp=$(date +%s)

# start Cassandra Cluster on the 5 compute nodes
# srun --jobid=$jobid start-cassandra.sh $timestamp &
srun --exclude=$(hostname) -N 5 -n 5 --ntasks-per-node=1 start-cassandra.sh $timestamp &

output_dir=$HOME/output/cassandra/$timestamp

mkdir -p $output_dir

IFS=',', read -ra nodes <<< "$other_ipstring"
ipsarr=()
for node in "${nodes[@]}"; do
    ipsarr+=("'$node'")
done
ip_string=$(IFS=,; echo "[${ipsarr[*]}]")
echo $ip_string

for file in $HOME/junchen/cs5424-project/cassandra/dataload/load_data_*.py; do
    sed -i.bak "s/Cluster(\[[^]]*\])/Cluster($ip_string)/g" "$file"
done

rm $HOME/junchen/cs5424-project/cassandra/dataload/*.bak

for file in $HOME/junchen/cs5424-project/cassandra/xacts/*.py; do
    sed -i.bak "s/Cluster(\[[^]]*\])/Cluster($ip_string)/g" "$file"
done

rm $HOME/junchen/cs5424-project/cassandra/xacts/*.bak

echo "sleep 120"
sleep 120

i=0
while [ $i -lt 20 ]; do
    file="$output_dir/$i.out"
    if [ -e "$file" ]; then
        echo "File '$file' exists."
        echo $(head -n 1 $file | xargs -I{} echo "$i,{}") | tee -a $output_dir/clients.csv
        ((i++))
    else
        echo "File '$file' does not exist. Waiting for 1 minute..."
        sleep 60
    fi
done

cat $output_dir/clients.csv | awk -F ',' '{ print $4 }' | awk -F, '
BEGIN { min = 9999999; sum = 0; max = -9999999; }
{
    for (i = 1; i <= NF; i++) {
        if ($i < min) min = $i;
        if ($i > max) max = $i;
        sum += $i;
    }
}
END { printf "%.2f,%.2f,%.2f", min, sum/NR, max; }
' > $output_dir/throughput.csv

# # Cleanup
# echo "Removing $output_dir/*.out"
# rm $output_dir/*.out

# synchronize to terminate all the compute nodes
finish_file="$output_dir/finish.out"
# echo "all done" > "$finish_file"
while true; do
    if [ -e "$finish_file" ]; then
        break
    else
        echo $my_ip: hanging...
        sleep 60
    fi
done

