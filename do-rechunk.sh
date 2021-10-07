#!/usr/bin/env bash 
#SBATCH -p hpc-demand 
#SBATCH --output=logs-slurm/job_%j_imerg-fwi.zarr_single_time_smaller_spatial_chunks_time001_lat010_lon050.out 
#SBATCH --error=logs-slurm/job_%j_imerg-fwi.zarr_single_time_smaller_spatial_chunks_time001_lat010_lon050.err 
set -e 
echo "*** Start time: $(date) *** " 
echo "Current directory: $(pwd)" 
module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj 
eval "$(conda shell.bash hook)" 
conda activate /backup/dieumynguyen/.conda/envs/eisfire 
echo "Using Python: $(which python)" 
python rechunk_single.py --strat_description="single_time_smaller_spatial_chunks" --input_path="eis-dh-fire/imerg-fwi.zarr" --timechunk="1" --xchunk="50" --ychunk="10" --data_variable="None" 
echo "*** End time: $(date) *** " 
