import os
import glob
import s3fs 
import time
import boto3
import shutil
import argparse
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from memory_profiler import memory_usage

import multiprocessing as mp
from typing import List, Any, Tuple, Dict

print(f'CPU count: {mp.cpu_count()}')
N_WORKERS = mp.cpu_count()

def execute(data):
    data.compute()

def measure_cpu_time(data):
    # Return sum of the system and user CPU time
    start = time.process_time()
    execute(data)
    cpu_time = time.process_time() - start
    return cpu_time

def measure_wall_time(data):
    # Measure wall clock time 
    start = time.time()
    execute(data)
    wall_time = time.time() - start
    return wall_time

def measure_peak_memory(execute_function, data):
    # Mem over time, over chunks of 0.1 sec by default
    # Units: Mebibyte
    mem_usage = memory_usage((execute_function, (data,), ))
    return max(mem_usage)

def measure_num_chunks(data):
    # Might need to edit for drawing different things
    n_dims = len(data.chunks)
    num_chunk = np.product([len(data.chunks[dim_i]) for dim_i in range(n_dims)])
    return num_chunk

def measure_chunk_size(data, num_chunk):
    # Return chunk size in memory
    # Units: byte
    chunk_size = data.nbytes / num_chunk
    return chunk_size 

def measure_wasted_mem_1(data_series, chunk_size, num_chunk):
    final_object = data_series.compute()
    wasted_mem = (chunk_size * num_chunk) - final_object.nbytes
    return wasted_mem

def measure_wasted_mem_2(data_series, peak_memory):
    final_object = data_series.compute()
    final_object_size = final_object.values.nbytes # Seems to be same as final_object.nbytes
    
    # Convert mebibytes to bytes
    peak_memory_bytes = peak_memory * 1048576 # = mebibyte * (8x1024x1024) / 8
    
    wasted_mem = peak_memory_bytes - final_object_size
    return wasted_mem
    
def measure_wasted_mem_3(select_data, data_series):
    # (Chunk size of original dataset * Num chunks of sub dataset) - Final object size
    original_chunk_size = select_data.nbytes / measure_num_chunks(select_data)
    sub_num_chunks = measure_num_chunks(data_series)
    
    final_object = data_series.compute()
    final_object_size = final_object.values.nbytes
    
    wasted_mem = (original_chunk_size * sub_num_chunks) - final_object_size
    return wasted_mem

def measure_strategy(data_path, dataset, variable, task='time', 
                    date=None, lat=None, lon=None, method=None,
                    num_trials=3, avg_aggregate=False):
    # Process file name
    if dataset == 'imerg-fwi':
        strategy_str = data_path.split('/')[-2] 
        category = '_'.join(strategy_str.split('_')[:-3])
        strategy = '_'.join(strategy_str.split('_')[-3:])
    else:
        strategy_str = data_path.split('/')[-3] 
        category = ''
        strategy = '_'.join(strategy_str.split('_'))

    time_chunk = strategy.split('_')[0].split('time')[-1]
    lat_chunk = strategy.split('_')[1].split('lat')[-1]
    lon_chunk = strategy.split('_')[2].split('lon')[-1]

    # Open data and perform selection
    s3 = s3fs.S3FileSystem(anon=False)
    if dataset == 'imerg-fwi':
        data_path = os.path.join(data_path, f'{dataset}.zarr')
    data_path = s3.get_mapper(data_path)
    data = xr.open_zarr(data_path, consolidated=True).unify_chunks() 

    select_data = data[variable]

    select_data['time'] = np.sort(select_data['time'].values)
    
    if task == 'time':
        data_series = select_data.sel(lat=lat, lon=lon, method=method)
        if avg_aggregate: 
            # Take avg over the spatial dimensions, 
            # e.g. producing array with shape (5136,) instead of (5136, 17, 14)	
            data_series = data_series.mean(dim=['lat', 'lon'])
    elif task == 'map':
        data_series = select_data.sel(time=date)
        if avg_aggregate:
            # Take avg over the time dimension, 
            # e.g. producing array with shape (721, 1152) instead of (6, 721, 1152)
            data_series = data_series.mean(dim=['time'])
    elif task == 'map_one_timestep':
        data_series = select_data.sel(time=date).isel(time=0)
    
    # Measure performance 
    cpu_time_list = []
    wall_time_list = []
    peak_memory_list = []
    num_chunk_list = []
    chunk_size_list = []
    wasted_mem_1_list = []
    wasted_mem_2_list = []
    wasted_mem_3_list = []
    for n in range(num_trials):
        cpu_time_trial = measure_cpu_time(data_series)
        wall_time_trial = measure_wall_time(data_series)
        peak_memory_trial = measure_peak_memory(execute, data_series)
        num_chunk_trial = measure_num_chunks(data_series)
        chunk_size_trial = measure_chunk_size(data_series, num_chunk_trial)
        wasted_mem_1_trial = measure_wasted_mem_1(data_series, chunk_size_trial, num_chunk_trial)
        wasted_mem_2_trial = measure_wasted_mem_2(data_series, peak_memory_trial)
        wasted_mem_3_trial = measure_wasted_mem_3(select_data, data_series)

        cpu_time_list.append(cpu_time_trial)
        wall_time_list.append(wall_time_trial)
        peak_memory_list.append(peak_memory_trial)
        num_chunk_list.append(num_chunk_trial)
        chunk_size_list.append(chunk_size_trial)
        wasted_mem_1_list.append(wasted_mem_1_trial)
        wasted_mem_2_list.append(wasted_mem_2_trial)
        wasted_mem_3_list.append(wasted_mem_3_trial)

    # Record avg of each metric
    cpu_time = np.mean(cpu_time_list)
    wall_time = np.mean(wall_time_list)
    peak_memory = np.mean(peak_memory_list)
    num_chunk = np.mean(num_chunk_list)
    chunk_size = np.mean(chunk_size_list)
    wasted_mem_1 = np.mean(wasted_mem_1_list)
    wasted_mem_2 = np.mean(wasted_mem_2_list)
    wasted_mem_3 = np.mean(wasted_mem_3_list)

    # Get number of data points in array
    array_shape = data_series.shape

    metrics_list = [category, time_chunk, lon_chunk, lat_chunk, 
                    cpu_time, wall_time, peak_memory, num_chunk, 
                    chunk_size, wasted_mem_1, wasted_mem_2, 
                    wasted_mem_3, array_shape]
    
    print(f'Category: {category} - Time chunk: {time_chunk} - Longitude chunk: {lon_chunk} - Latitude chunk: {lat_chunk} -- ' \
          f'cpu time: {cpu_time:0.2f} sec, wall time: {wall_time:0.2f} sec, ' \
          f'peak mem: {peak_memory:0.2f} MiB, num chunks: {num_chunk}, ' \
          f'chunk size: {chunk_size:0.2f} B, wasted mem 1: {wasted_mem_1} B, ' \
          f'wasted mem 2: {wasted_mem_2} B, wasted mem 3: {wasted_mem_3} B, ' \
          f'array shape: {array_shape}')

    return metrics_list

def create_batches(all_strategies: List[str], n_workers: int = 1) -> Tuple[List[str]]:
    n_strategies = len(all_strategies)
    batch_size = n_strategies // n_workers
    remainder = n_strategies % n_workers
    batched_strategies = []
    for batch_i in range(n_workers):
        idx_i = batch_i*batch_size
        idx_j = (batch_i + 1) * batch_size
        strategy_i = all_strategies[idx_i : idx_j]
        batched_strategies.append(strategy_i)
    
    if remainder:
        strategy_remainder = all_strategies[idx_j:]
        assert remainder == len(strategy_remainder)

        for i, strat_r in enumerate(strategy_remainder):
            worker_index = i % n_workers
            batched_strategies[worker_index].append(strat_r)

    return batched_strategies

def worker_fxn(i, strategies, dataset, variable, metrics, 
               task, date, lat, lon, method, num_trials, 
               avg_aggregate):
    for data_path in strategies:   
        try:
            metrics_list = measure_strategy(data_path, dataset, variable, 
                                            task=task, date=date, lat=lat, 
                                            lon=lon, method=method, 
                                            num_trials=num_trials, 
                                            avg_aggregate=avg_aggregate)        
            metrics['categories'] = metrics['categories'] + [metrics_list[0]]
            metrics['time_chunks'] = metrics['time_chunks'] + [metrics_list[1]]
            metrics['lon_chunks'] = metrics['lon_chunks'] + [metrics_list[2]]
            metrics['lat_chunks'] = metrics['lat_chunks'] + [metrics_list[3]]
            metrics['cpu_times'] = metrics['cpu_times'] + [metrics_list[4]]
            metrics['wall_times'] = metrics['wall_times'] + [metrics_list[5]]
            metrics['peak_memories'] = metrics['peak_memories'] + [metrics_list[6]]
            metrics['num_chunks'] = metrics['num_chunks'] + [metrics_list[7]]
            metrics['chunk_sizes'] = metrics['chunk_sizes'] + [metrics_list[8]]
            metrics['wasted_memories_1'] = metrics['wasted_memories_1'] + [metrics_list[9]]
            metrics['wasted_memories_2'] = metrics['wasted_memories_2'] + [metrics_list[10]]
            metrics['wasted_memories_3'] = metrics['wasted_memories_3'] + [metrics_list[11]]
            metrics['array_shape'] = metrics['array_shape'] + [metrics_list[12]]
        except Exception as e:
            print(f'Error at: {data_path}')
            print(e)
            continue

def run(all_strategies, savename, dataset, variable, 
        task='time', date=None, lat=None, lon=None, 
        method=None, num_trials=3, avg_aggregate=False):
    manager = mp.Manager()
    metrics = manager.dict()
    metrics['categories'] = []
    metrics['time_chunks'] = []
    metrics['lon_chunks'] = []
    metrics['lat_chunks'] = []
    metrics['cpu_times'] = []
    metrics['wall_times'] = []
    metrics['peak_memories'] = []
    metrics['num_chunks'] = []
    metrics['chunk_sizes'] = []
    metrics['wasted_memories_1'] = []
    metrics['wasted_memories_2'] = []
    metrics['wasted_memories_3'] = []
    metrics['array_shape'] = []

    # Create batches of filenames
    batched_strategies = create_batches(all_strategies, n_workers=N_WORKERS)

    # Run workers, update metrics dict
    processes = []
    for worker_i, strategies in enumerate(batched_strategies):
        proc = mp.Process(target=worker_fxn, args=(worker_i, strategies,
                                   dataset, variable, metrics, task, date, 
                                   lat, lon, method, num_trials, avg_aggregate))
        proc.start()
        processes.append(proc)

    [p.join() for p in processes]
    print('Finished measuring all strategies.')

    print('Saving data.')
    metrics_df = pd.DataFrame(dict(metrics))
    metrics_df.to_csv(f'data/geos-global-fp/{savename}_metrics.csv')

def main():
    #------ Set up paths ------#
    dataset_dict = {
        0: 'imerg-fwi',
        1: 'geos-fp-global_inst'
    }
    bucket = 'eis-dh-fire'
    dataset = dataset_dict[1]
    folder = f'dieumynguyen_rechunked/{dataset}/'
    
    #------ Choose a variable ------#
    variable = 'IMERG.FINAL.v6_FWI' if dataset=='imerg-fwi' else 'BCEXTTAU' 

    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=folder, Delimiter='/')

    #------ Find all data folders in bucket ------#
    all_strategies = []
    for prefix in result.get('CommonPrefixes'):
        folder_path = prefix.get('Prefix')
        if dataset == 'imerg-fwi':
            folder_path = os.path.join(bucket, folder_path)
        else:
            folder_path = os.path.join(bucket, folder_path, 'inst.zarr/')
        all_strategies.append(folder_path)
    
    #### DEBUG ####
    # all_strategies = all_strategies[:3]
    #### DEBUG ####

    print(f'Num of strategies: {len(all_strategies)}')

    #------ Choose a task ------#
    # 0: time series, 1: time series over region, 2: map over time, 3: map over 1 timestep
    TASK = 2
    num_trials = 10
    print(f'Num trials: {num_trials}')

    #------ Run task ------#
    if TASK == 0:
        print(f'Drawing time series at single coordinate.')
        lat = 47.61
        lon = -122.19
        method = 'nearest'
        run(all_strategies, 'time_series', dataset, variable, 
            task='time', date=None, lat=lat, lon=lon, method=method, 
            num_trials=num_trials, avg_aggregate=False)

    elif TASK == 1:
        # BBox CSV format from: https://boundingbox.klokantech.com/
        bbox_dict = {
            'ohio':          [-84.91,38.21,-80.5,42.25],
            'california':    [-124.92,32.64,-114.25,42.11],
            'usa':           [-124.9,24.9,-66.7,49.4],
            'north_america': [-168.0,15.3,-53.0,71.3]
        }
        bbox_type = 'north_america'
        print(f'Drawing time series over region: {bbox_type}.')
        bbox = bbox_dict[bbox_type] 
        lon_slice = slice(bbox[0], bbox[2])
        lat_slice = slice(bbox[1], bbox[3])
        run(all_strategies, f'time_series_over_region_{bbox_type}', 
            dataset, variable, task='time', date=None, 
            lat=lat_slice, lon=lon_slice, method=None, 
            num_trials=num_trials, avg_aggregate=True)

    elif TASK == 2:
        time_dict = {
            '6_hr':    ('2020-06-01T00', '2020-06-01T05'),
            '12_hr':   ('2020-06-01T00', '2020-06-01T11'),
            '1_day':   ('2020-06-01', '2020-06-01'),
            '2_day':   ('2020-06-01', '2020-06-02'),
            '7_day':   ('2020-06-01', '2020-06-07'),
            '30_day':  ('2020-06-01', '2020-06-30'),
            '60_day':  ('2020-06-01', '2020-07-30'),
            '180_day': ('2020-06-01', '2020-11-27'),
        }
        time_type = '30_day'
        print(f'Drawing map over time: {time_type}.')
        time_range = time_dict[time_type]
        date_start = time_range[0]
        date_end = time_range[1]
        time_bounds = slice(date_start, date_end)
        run(all_strategies, f'map_over_time_{time_type}', dataset, variable, 
            task='map', date=time_bounds, lat=None, lon=None, method=None, 
            num_trials=num_trials, avg_aggregate=True)
    
    elif TASK == 3:
        print(f'Drawing map at one timestep.')
        date = '2020-06-01'
        run(all_strategies, 'map_one_timestep', dataset, variable, 
            task='map_one_timestep', date=date, lat=None, lon=None, method=None, 
            num_trials=num_trials, avg_aggregate=False)

if __name__ == '__main__':
    main()