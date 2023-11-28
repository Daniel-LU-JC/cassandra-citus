# Wholesale Citus
This is the Wholesale project implemented with Citus.

## Setup and Deployment

1. Download and extract project file for the necessary transactions and data files:

```
cd $HOME
wget https://www.comp.nus.edu.sg/~cs4224/project_files.zip
unzip project_files.zip
```

2. Download and extract the files required to start cassandraDB

```
cd $HOME
unzip O_citus.zip
```

3. Run the build and install script for Citus

```
$HOME/project_files/scripts/install-citus.sh
```

4. Submit a SLURM batch job with the `slurm_server-cmd.sh` script

```
cd $HOME/O_citus
sbatch slurm_server_cmd.sh
```

This will create the tables, load the data and run all the transactions.


## Performance Measurement

- Once the bash script is run, there will be an output in the ~/output/citusdb
- Choose the folder with the largest timestamp (most recent)
- Edit the gen_throughput.sh `output_dir=$HOME/output/citusdb/timestamp` replace the timestamp with the folder largest timestamp e.g `output_dir=$HOME/output/citusdb/1699621888` 
- run `sh gen_throughput.sh` 

