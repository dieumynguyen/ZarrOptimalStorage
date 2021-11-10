import itertools
from subprocess import call

PY_FILE = 'rechunk_single.py'
BASH_FILE = 'do-rechunk.sh'

def create_bash_script(params):
    strat_description = params['strat_description']
    input_path = params['input_path']
    timechunk = params['timechunk']
    xchunk = params['xchunk']
    ychunk = params['ychunk']
    data_variable = params['data_variable']
    dataset = input_path.split('/')[1]
    strategy = f'{dataset}_{strat_description}_time{timechunk:04d}_lat{ychunk:04d}_lon{xchunk:04d}'

    with open(BASH_FILE, "w") as outfile:
        outfile.write(f'#!/usr/bin/env bash \n')
        outfile.write(f'#SBATCH -p hpc-demand \n') 
        # outfile.write(f'#SBATCH --exclusive \n') 
        outfile.write(f'#SBATCH --ntasks=1 \n') 
        # outfile.write(f'#SBATCH --mem=30gb \n') 
        outfile.write(f'#SBATCH --output=logs-slurm/job_%j_{strategy}.out \n') 
        outfile.write(f'#SBATCH --error=logs-slurm/job_%j_{strategy}.err \n') 
        outfile.write(f'set -e \n') 
        outfile.write(f'echo "*** Start time: $(date) *** " \n') 
        outfile.write(f'echo "Current directory: $(pwd)" \n') 
        outfile.write(f'module load miniconda3-4.9.2-gcc-11.1.0-i4wwucj \n') 
        outfile.write(f'eval "$(conda shell.bash hook)" \n') 
        outfile.write(f'conda activate /backup/dieumynguyen/.conda/envs/eisfire \n') 
        outfile.write(f'echo "Using Python: $(which python)" \n') 
        outfile.write(f'python {PY_FILE} --strat_description="{strat_description}" --input_path="{input_path}" --timechunk="{timechunk}" --xchunk="{xchunk}" --ychunk="{ychunk}" --data_variable="{data_variable}" \n')
        outfile.write(f'echo "*** End time: $(date) *** " \n') 
    return BASH_FILE 

def main():
    # Note: 999 = all
    time = [1] # [1, 6, 12, 24, 48, 120, 720, 1440, 2160, 999]
    lat = [50]
    lon = [100]
    param_sets = list(itertools.product(time,lon,lat))

    param_list = []
    for p in param_sets:
        p_dict = {
            # CATEGORIES OF STRATEGIES:
            # all_times_smaller_spatial_chunks
            # global_combined_timesteps
            # global_one_timestep
            # hybrid
            # single_time_smaller_spatial_chunks
            'strat_description': 'hybrid',
            'input_path': 'eis-dh-fire/geos-fp-global/inst.zarr',  
            # 'input_path': 'eis-dh-fire/imerg-fwi.zarr',
            'timechunk': p[0],
            'xchunk': p[1],
            'ychunk': p[2],
            # 'data_variable': 'None',
            'data_variable': 'BCEXTTAU' 
        }
        param_list.append(p_dict)

    for params_i, params in enumerate(param_list):
        print(f'Processing chunking strategy {params_i} / {len(param_list)-1} \n')
        bash_file = create_bash_script(params)
        call(["sbatch", bash_file])

if __name__ == '__main__':
    main()
