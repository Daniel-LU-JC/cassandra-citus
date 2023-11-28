# Wholesale CassandraDB
This is the Wholesale project implemented with CassandraDB.

## Deployment

1. Download and extract project file for the necessary transactions and data files:

```
cd $HOME
wget https://www.comp.nus.edu.sg/~cs4224/project_files.zip
unzip project_files.zip
```

2. Download and extract the files required to start cassandraDB

```
cd $HOME
unzip O_cassandra.zip
```

3. Download and extract CassandraDB

```
cd $HOME/O_cassandra
wget https://dlcdn.apache.org/cassandra/4.1.3/apache-cassandra-4.1.3-bin.tar.gz
tar -xvzf apache-cassandra-4.1.3-bin.tar.gz
```

4. Submit a SLURM batch job with our `entrypoint.sh` script

```
cd $HOME/O_cassandra
sbatch entrypoint.sh
```
