#!/usr/bin/env bash 
#SBATCH --output=logs-slurm/job_%j_upload_to_s3.out 
#SBATCH --error=logs-slurm/job_%j_upload_to_s3.err 
set -e 
echo "*** Start time: $(date) *** " 
echo "Current directory: $(pwd)" 
module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj 
eval "$(conda shell.bash hook)" 
conda activate /backup/dieumynguyen/.conda/envs/eisfire 
echo "Using Python: $(which python)" 
python upload_to_s3.py
echo "*** End time: $(date) *** " 
