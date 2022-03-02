# Optimal Strategies for Storing Earth Science Datasets in the Commercial Cloud

## Overview:
This repo provides the code for the study on the optimal chunking strategies of multi-dimensional data in Zarr format for storage in the cloud (AWS S3). Code is provided for rechunking a default Zarr archive, measuring the performance (mainly time and memory consumption) of different chunking strategies, and performance data visualization. This study was conducted as part of the Fall 2021/Spring 2022 internship at NASA Goddard Space Flight Center.

## Requirements:
The complete list of required packages is provided in *env-eisfire.yml*, which you can install in your environment via [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) with the command `conda env create -f env-eisfire.yml`. Note that this code is set up on a cluster on AWS that uses [slurm](https://slurm.schedmd.com/documentation.html). 

## Usage:
### 1. Rechunk dataset
##### Input:
In this study, we use the [GEOS-FP](https://gmao.gsfc.nasa.gov/pubs/docs/Lucchesi1203.pdf) dataset in Zarr format stored in the AWS S3 bucket `eis-dh-fire/geos-fp-global/`; speficially, the inst.zarr store and BCEXTTAU variable. The default chunking scheme: 5136 chunks in the `time` dimension, 1 chunk in `longitude`, and 1 chunk in `latitude`.
##### Usage:
To rechunk the dataset into a different scheme (e.g., 5136 chunks in time, 100 in longitude, and 100 in latitude), nagivate to the directory `rechunk/` and modify the `main()` function in the script `run_rechunk.py` for the variables `time`, `lat`, and `lon` to take on desired values (single value or a list of values for each variable - the script will create unique combinations of the variables). Run the rechunking script with the command: `python run_rechunk.py` to automatically launch a cluster job for each combination of variable values. 
##### Output:
Job info and progress as well as any errors are stored in the `.out` and `.err` files in the sub-directory `logs-slurm/`. The final output Zarr store is back written to S3 (`eis-dh-fire/dieumynguyen_rechunked/geos-fp-global_inst/`).

### 2. Measure performance
##### Input & Info: 
After rechunking the dataset to various chunking schemes and storing the different versions of the dataset on S3, we track how the schemes perform for common data access and analysis operations (e.g., extracting a time series at a location or extracting a map or spatial slice at a datetime). Performance metrics include CPU time, peak memory usage, the rechunking time, and Zarr store archive size. 
##### Usage:
Nagivate to directory `measure_performance/`.
- To obtain archive size data, run `sbatch measure_archive_size.sh` to submit a cluster job, which runs `measure_archive_size.py`.
- To obtain rechunking time, run `sbatch measure_rechunking_time.sh` to submit a cluster job, which runs `measure_rechunking_time.py`.
- To obtain CPU time and peak memory usage for a given data operation, modify the selected operation in the `main()` function in `measure_performance.py`. Then, run `sbatch measure_performance.sh` to submit a cluster job, which runs `measure_performance.py`.
##### Output:
Job info and progress as well as any errors are stored in the `.out` and `.err` files in the sub-directory `performance-logs-slurm/`. 
- Archive size data is saved in `data/geos-fp-global_inst/archive_sizes.csv`.
- Rechunking time data is saved in `data/geos-fp-global_inst/rechunking_time.csv`.
- Time and memory data for each operation are saved in `data/geos-fp-global_inst` with filename indicating the operation and number of trials/repetitions (e.g., `time_series_metrics_ntrials1.csv`).

### 3. Data visualization
##### Input:
The performance data generated in #2. 
##### Usage:
Nagivate to directory `visualization/`. Run `sbatch visualize.sh` to submit a job to run `visualize.py`.
##### Output:
Heatmaps and scatterplots shown in paper, stored in `data/geos-fp-global_inst/heatmaps`, `data/geos-fp-global_inst/normalized_heatmaps`, and `data/geos-fp-global_inst/scatterplots`.

Reference: Nguyen DMT, Cortes JC, Dunn MM, Shiklomanov AN (2022). Optimal Strategies for Storing Earth Science Datasets in the Commercial Cloud. 

----------------------------