#!/usr/bin/env bash 
#SBATCH -p hpc-demand
#SBATCH --output=logs-slurm/job_%j_measure_size.out 
#SBATCH --error=logs-slurm/job_%j_measure_size.err 
set -e 
echo "*** Start time: $(date) *** " 
echo "Current directory: $(pwd)" 
module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj 
eval "$(conda shell.bash hook)" 
conda activate /backup/dieumynguyen/.conda/envs/eisfire 
echo "Using Python: $(which python)" 
python measure_archive_size.py
echo "*** End time: $(date) *** " 
