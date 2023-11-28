#!/bin/bash

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <comma-separated-hostnames>"
    exit 1
fi

echo "Checking if psycopg2 binary library exist, if not exist install"
if pip show psycopg2-binary &> /dev/null; then
    echo "psycopg2-binary is installed."
else
    echo "psycopg2-binary is not installed. Installing"
    pip install psycopg2-binary
fi

timestamp=$(date +%s)

# Submit slurm job to start citusdb server and run clients
srun start-citus.sh $1 $timestamp &

output_dir=$HOME/output/citusdb/$timestamp

mkdir -p $output_dir

i=0

sleep 60
echo master sleep done

hostnames=$1
IFS=',' read -ra hosts <<< "$hostnames"
if [[ ${hosts[0]} =~ \[([0-9]+)-([0-9]+)\] ]]; then
    prefix="${hosts[0]%%\[*}"
    start="${BASH_REMATCH[1]}"
    coordinator_ip=$prefix$start
    echo "coordi if: $coordinator_ip"
else
    coordinator_ip=${hosts[0]}
    echo "coordi else: $coordinator_ip"
fi

echo "coordi: $coordinator_ip"

echo "Initialising tables"
python $HOME/citusdb/create_table.py "$coordinator_ip"
python $HOME/citusdb/init_distribute_table.py "$coordinator_ip"
for i in $(seq 1 7); do
    python $HOME/citusdb/load_data.py "$(($index))" $coordinator_ip
done
echo "create done" > $output_dir/create_table.out

i=0
while [ $i -lt 20 ]; do
    file="$output_dir/$i.out"
    if [ -e "$file" ]; then
        echo "File '$file' exists."
        echo $(head -n 1 $file | xargs -I{} echo "$i,{}") | tee -a $output_dir/clients.csv
        ((i++))
    else
        echo "File '$file' does not exist. Waiting for 5 seconds..."
        sleep 5
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
cd $output_dir
python $HOME/citusdb/transactions/db_state.py "$coordinator_ip"
echo "DB State done"
finish_file="$output_dir/finish.out"
echo "all done" > "$finish_file"
