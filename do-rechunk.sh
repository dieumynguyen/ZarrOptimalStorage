#!/usr/bin/env bash 
#SBATCH -p hpc-demand 
#SBATCH --ntasks=1 
#SBATCH --output=logs-slurm/job_%j_geos-fp-global_hybrid_time0001_lat0050_lon0100.out 
#SBATCH --error=logs-slurm/job_%j_geos-fp-global_hybrid_time0001_lat0050_lon0100.err 
set -e 
echo "*** Start time: $(date) *** " 
echo "Current directory: $(pwd)" 
module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj 
eval "$(conda shell.bash hook)" 
conda activate /backup/dieumynguyen/.conda/envs/eisfire 
echo "Using Python: $(which python)" 
python rechunk_single.py --strat_description="hybrid" --input_path="eis-dh-fire/geos-fp-global/inst.zarr" --timechunk="1" --xchunk="100" --ychunk="50" --data_variable="BCEXTTAU" 
echo "*** End time: $(date) *** " 
