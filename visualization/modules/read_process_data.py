import pandas as pd
from ast import literal_eval

def prod(val) : 
    res = 1 
    for ele in val: 
        res *= ele 
    return res  

def read_process_csv(filepath, timeall, lonall, latall, sort_by='lonlat'):
    metrics_df = pd.read_csv(filepath, index_col=0)
        
    # Replace 'all' and convert units
    metrics_df.time_chunks = metrics_df.time_chunks.replace('all', timeall).astype('int')
    metrics_df.lon_chunks = metrics_df.lon_chunks.replace('all', lonall).astype('int')
    metrics_df.lat_chunks = metrics_df.lat_chunks.replace('all', latall).astype('int')

    # Add product column for latxlon
    metrics_df['lon x lat'] = [f'{lon}x{lat}' for lon, lat in zip(metrics_df.lon_chunks, metrics_df.lat_chunks)]
    metrics_df['lonlat_product'] = [lon*lat for lon, lat in zip(metrics_df.lon_chunks, metrics_df.lat_chunks)]
    
    # Another column of timexlatxlon
    metrics_df['time x lon x lat'] = [f'{time}x{lon}x{lat}' for time, lon, lat in zip(metrics_df.time_chunks, metrics_df.lon_chunks, metrics_df.lat_chunks)]
    metrics_df['total_product'] = [lon*lat*time for time, lon, lat in zip(metrics_df.time_chunks, metrics_df.lon_chunks, metrics_df.lat_chunks)]

    # Sort
    if sort_by == 'lonlat':
        metrics_df = metrics_df.sort_values(by='lonlat_product')
    elif sort_by == 'timelonlat':
        metrics_df = metrics_df.sort_values(by='total_product')
        
    # In case of any duplicate rows
    metrics_df.drop_duplicates(subset='time x lon x lat', keep='first', inplace=True)
    
    try:
        # Convert array_shape column to number
        num_data_points = prod(list(literal_eval(metrics_df.array_shape[0])))
        metrics_df['num_data_points'] = [num_data_points] * len(metrics_df)

        # Add normalized columns to df
        # 1. Normalized cpu time
        metrics_df['norm_cpu_times'] = metrics_df.num_data_points / metrics_df.cpu_times

        # 2. Normalized wall time
        metrics_df['norm_wall_times'] = metrics_df.num_data_points / metrics_df.wall_times

        # 3. Normalized peak memory
        metrics_df['norm_peak_memories'] =  metrics_df.peak_memories / metrics_df.num_data_points
    except:
        pass
    
    # Save processed df - overwrite old file, unless need to save separately later
    savepath = filepath
    metrics_df.to_csv(savepath)

    return metrics_df