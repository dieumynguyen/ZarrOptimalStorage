#!/usr/bin/env bash 
#SBATCH --exclusive
#SBATCH -p hpc-demand
#SBATCH --output=performance-logs-slurm/job_%j.out 
#SBATCH --error=performance-logs-slurm/job_%j.err 
set -e 
echo "*** Start time: $(date) *** " 
echo "Current directory: $(pwd)" 
module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj 
eval "$(conda shell.bash hook)" 
conda activate /backup/dieumynguyen/.conda/envs/eisfire 
echo "Using Python: $(which python)" 
python measure_performance.py
echo "*** End time: $(date) *** " 
