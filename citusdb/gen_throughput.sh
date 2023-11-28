output_dir=$HOME/output/citusdb/1699604666

for i in $(seq 0 19); do
    file="$output_dir/$i.out"
    echo $(head -n 1 $file | xargs -I{} echo "$i,{}") | tee -a $output_dir/clients.csv
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
