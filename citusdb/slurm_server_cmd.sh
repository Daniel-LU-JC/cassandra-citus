#!/bin/bash
#SBATCH --job-name=run_citus_script
#SBATCH --output=slurm_citus.out
#SBATCH --error=slurm_citus.err
#SBATCH --nodes=5
#SBATCH --cpus-per-task=3
#SBATCH --nodelist=xgph[15-19]
#SBATCH --partition=long
#SBATCH --time=08:00:00
#SBATCH --mem=8G

./entrypoint.sh xgph[15-19]
