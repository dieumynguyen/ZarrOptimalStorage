import os
import glob
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.rcParams['font.family'] = "sans-serif"
mpl.rcParams['font.sans-serif'] = "Arial"

import modules.read_process_data as data_reader
import modules.plot_heatmap as heatmap_maker
import modules.plot_scatterplots as scatterplot_maker

def avg_aggregation(dataset, task_name, file_paths, all_lookup):
    all_metrics = []
    for filepath in file_paths:
        basename = os.path.basename(filepath)
        metrics_df = data_reader.read_process_csv(filepath, timeall=all_lookup['timeall'], 
                        lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                        sort_by='lonlat')
        metrics_df["path_name"] = basename
        all_metrics.append(metrics_df)
    all_metrics_df = pd.concat(all_metrics)

    df_list = []
    for group_key, df_i in all_metrics_df.groupby("time x lon x lat"):
        avg_df = df_i.mean()
        df_list.append(avg_df)
    mean_df = pd.DataFrame(df_list)

    # Put back the string columns to sort
    # Add product column for latxlon
    mean_df['lon x lat'] = [f'{int(lon)}x{int(lat)}' for lon, lat in zip(mean_df.lon_chunks, mean_df.lat_chunks)]
    mean_df['lonlat_product'] = [lon*lat for lon, lat in zip(mean_df.lon_chunks, mean_df.lat_chunks)]

    # Sort
    mean_df = mean_df.sort_values(by='lonlat_product')
    mean_df.time_chunks = mean_df.time_chunks.astype(int)
    mean_df.to_csv(f'../data/{dataset}/performance_data/{task_name}.csv')
    return mean_df

def make_heatmap(
        dataset,
        titles_dict, 
        filepath, 
        all_lookup, 
        KEY, 
        metrics_df=None,
        metrics_df_2=None,
        task_name=None,
        TRANSPOSE=False,
        VMIN=None,
        VMAX=None,
        QUANTILE_Q=0.65,
        NUM_DECIMALS=3,
        MB_TO_GB=True,
        SHOW_TITLE=False,
        PROCESS_CSV=True,
        PRODUCT=False
    ):

    if PROCESS_CSV:
        metrics_df = data_reader.read_process_csv(
            filepath, 
            timeall=all_lookup['timeall'], 
            lonall=all_lookup['lonall'], 
            latall=all_lookup['latall'],
            sort_by='lonlat'
            )
        task_name = '_'.join(filepath.split('/')[-1].split('.')[0].split('_')[:-1])

    results = heatmap_maker.make_pivot_tables(
        metrics_df_1=metrics_df, 
        metrics_df_2=metrics_df_2, 
        column_name=KEY, 
        convert_mb_to_gb=MB_TO_GB, 
        product=PRODUCT
        )

    sns.set(font_scale=2) # 1.5
    heatmap_maker.make_heatmap(
        dataset,
        titles_dict,
        results, 
        KEY, 
        task_name,
        transpose=TRANSPOSE, 
        vmin=VMIN, 
        vmax=VMAX,
        quantile_q=QUANTILE_Q,
        fontsize=12, # 10
        num_decimals=NUM_DECIMALS,
        convert_mb_to_gb=MB_TO_GB,
        show_title=SHOW_TITLE
    )

def organize_df(df):
    df['time_chunks'] = df['time_chunks'].astype(int)
    df['lon_chunks'] = df['lon_chunks'].astype(int)
    df['lat_chunks'] = df['lat_chunks'].astype(int)

    # Add product column for latxlon
    df['lon x lat'] = [f'{lon}x{lat}' for lon, lat in zip(df.lon_chunks, df.lat_chunks)]
    df['lonlat_product'] = [lon*lat for lon, lat in zip(df.lon_chunks, df.lat_chunks)]

    # Another column of timexlatxlon
    df['time x lon x lat'] = [f'{time}x{lon}x{lat}' for time, lon, lat in zip(df.time_chunks, df.lon_chunks, df.lat_chunks)]
    df['total_product'] = [lon*lat*time for time, lon, lat in zip(df.time_chunks, df.lon_chunks, df.lat_chunks)]
    df = df.sort_values(by='lonlat_product')
    df.drop_duplicates(subset='time x lon x lat', keep='first', inplace=True)
    return df

def main():
    ntrials = 1 
    dataset = 'geos-fp-global_inst' 

    # Lookup dictionaries
    titles_dict = {
        "chunk_sizes": "Chunk size (B)",
        "num_chunks": "Number of chunks",
        "peak_memories": "Peak memory (GB)", # GB for heatmaps, MiB for normalized
        "wall_times": "Wall time (Sec)",
        "norm_wall_times": "Data points per sec",
        "norm_peak_memories": "Memory per data point (MiB)",
        "runtime_hr": "Rechunking time (Hr)",
        "archive_size": "Archive size (GB)"
    }

    convert_all_dict = {
        'geos-fp-global_inst': {'timeall': 5136, 'lonall': 1152, 'latall': 721}
    }
    all_lookup = convert_all_dict[dataset]

    task_dict = {
        0: f'time_series_metrics_ntrials{ntrials}',
        1: f'map_one_timestep_metrics_ntrials{ntrials}'
    }       

    keys = ['norm_wall_times', 'norm_peak_memories', 'wall_times', 'peak_memories']

    pairwise_combinations = [
        ('chunk_sizes', 'wall_times'),
        ('chunk_sizes', 'peak_memories'),
        ('peak_memories', 'wall_times'),
    ]

    norm_pairwise_combinations = [
        ('chunk_sizes', 'norm_wall_times'),
        ('chunk_sizes', 'norm_peak_memories'),
        ('norm_peak_memories', 'norm_wall_times'),
    ]

    # Plot time series data
    print('Plotting time series data...')
    filepath = f'../data/{dataset}/performance_data/{task_dict[0]}.csv'
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            filepath, 
            all_lookup, 
            KEY=k
        )

    # Plot map data
    print('Plotting map data...')
    filepath = f'../data/{dataset}/performance_data/{task_dict[1]}.csv'
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            filepath, 
            all_lookup, 
            KEY=k
        )

    # Plot product/avg of both operations' data
    print('Plotting product of both operations...')
    # Product of time series and map df's
    time_filepath = f'../data/{dataset}/performance_data/{task_dict[0]}.csv'
    map_filepath = f'../data/{dataset}/performance_data/{task_dict[1]}.csv'
    time_metrics_df = data_reader.read_process_csv(time_filepath, timeall=all_lookup['timeall'], 
                        lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                        sort_by='lonlat')
    map_metrics_df = data_reader.read_process_csv(map_filepath, timeall=all_lookup['timeall'], 
                        lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                        sort_by='lonlat')
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            time_filepath, 
            all_lookup, 
            KEY=k, 
            metrics_df=time_metrics_df,
            metrics_df_2=map_metrics_df,
            task_name='average' if 'norm' in k else 'product',
            PROCESS_CSV=False,
            PRODUCT=False if 'norm' in k else True
        )
        
    # Plot time series aggregation data
    print('Plotting time series aggregation data...')
    file_paths = np.sort(glob.glob(f'../data/{dataset}/performance_data/time_series_over_region_*_ntrials{ntrials}.*'))
    task_name = 'time_series_average'
    mean_df = avg_aggregation(dataset, task_name, file_paths, all_lookup)
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            '', 
            all_lookup, 
            KEY=k, 
            metrics_df=mean_df,
            task_name=task_name,
            QUANTILE_Q=0.7,
            NUM_DECIMALS=3,
            PROCESS_CSV=False
        )

    # Plot map aggregation data
    print('Plotting map aggregation data...')
    file_paths = np.sort(glob.glob(f'../data/{dataset}/performance_data/map_over_time_*_ntrials{ntrials}.*'))
    task_name = 'maps_average'
    mean_df = avg_aggregation(dataset, task_name, file_paths, all_lookup)
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            '', 
            all_lookup, 
            KEY=k, 
            metrics_df=mean_df,
            task_name=task_name,
            QUANTILE_Q=0.7,
            NUM_DECIMALS=3,
            PROCESS_CSV=False
        )

    # Plot product/avg of both operations' data aggregations
    print('Plotting product of both aggregation operations...')
    # Product of time series and map df's
    time_filepath = f'../data/{dataset}/performance_data/time_series_average.csv'
    map_filepath = f'../data/{dataset}/performance_data/maps_average.csv'
    time_metrics_df = data_reader.read_process_csv(time_filepath, timeall=all_lookup['timeall'], 
                        lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                        sort_by='lonlat')
    map_metrics_df = data_reader.read_process_csv(map_filepath, timeall=all_lookup['timeall'], 
                        lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                        sort_by='lonlat')
    for k in keys:
        make_heatmap(
            dataset,
            titles_dict, 
            time_filepath, 
            all_lookup, 
            KEY=k, 
            metrics_df=time_metrics_df,
            metrics_df_2=map_metrics_df,
            task_name='average_aggregate' if 'norm' in k else 'product_aggregate',
            PROCESS_CSV=False,
            PRODUCT=False if 'norm' in k else True
        )

    # Plot rechunking time
    print('Plotting rechunking time...')
    filepath = f'../data/{dataset}/performance_data/rechunking_time.csv'
    df = pd.read_csv(filepath)
    df = df.drop_duplicates(['time_chunks', 'lon_chunks', 'lat_chunks'], keep='last')
    df['time_chunks']= df['time_chunks'].replace(999, 5136)
    df['lon_chunks'] = df['lon_chunks'].replace(999, 1152)
    df['lat_chunks'] = df['lat_chunks'].replace(999, 721)
    df = organize_df(df)
    task_name = 'rechunking'
    make_heatmap(
            dataset,
            titles_dict, 
            filepath, 
            all_lookup, 
            KEY='runtime_hr',
            metrics_df=df,
            task_name=task_name,
            QUANTILE_Q=0.82,
            PROCESS_CSV=False
        )

    # Plot archive size
    print('Plotting archive size...')
    filepath = f'../data/{dataset}/performance_data/archive_sizes.csv'
    df = pd.read_csv(filepath)
    df['archive_size'] = df['archive_size'] * 1e-9
    df['time_chunks'].replace('all', 5136, inplace=True)
    df['lon_chunks'].replace('all', 1152, inplace=True)
    df['lat_chunks'].replace('all', 721, inplace=True)
    df = organize_df(df)
    task_name = 'archive_size'
    make_heatmap(
            dataset,
            titles_dict, 
            filepath, 
            all_lookup, 
            KEY='archive_size',
            metrics_df=df,
            task_name=task_name,
            QUANTILE_Q=None,
            PROCESS_CSV=False
       )

    # Plot scatterplots
    print('Plotting scatterplots...')
    LOG = True
    NORMS = [False, True]
    task_name = 'overlay_tasks'
    task_dict = {
        0: f'time_series_metrics_ntrials{ntrials}',
        1: f'map_one_timestep_metrics_ntrials{ntrials}'
    }
    filepath = f'../data/{dataset}/performance_data/{task_dict[0]}.csv'
    map_filepath = f'../data/{dataset}/performance_data/{task_dict[1]}.csv'
    metrics_df = data_reader.read_process_csv(filepath, timeall=all_lookup['timeall'], 
                    lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                    sort_by='lonlat')
    map_metrics_df = data_reader.read_process_csv(map_filepath, timeall=all_lookup['timeall'], 
                      lonall=all_lookup['lonall'], latall=all_lookup['latall'],
                      sort_by='lonlat')
    concatenated = pd.concat([metrics_df.assign(dataset='set1'), map_metrics_df.assign(dataset='set2')])
    num_colors = len(metrics_df['lonlat_product'].unique())
    color_palette = sns.color_palette("inferno_r", as_cmap=True, n_colors=num_colors)
    color_palette = color_palette(np.linspace(0.1, 1, num_colors))
    color_palette = sns.color_palette(color_palette)
    
    for NORM in NORMS:
        pairs = norm_pairwise_combinations if NORM else pairwise_combinations

        lim1 = [(10**-2, 10**2.3), (10**2, 4.5*(10**3)), (10**-2, 10**2.3)]
        lim2 = [(10**1.5, 10**7.8), (10**-4, 10**0.2), (10**1.4, 10**8)]
        YLIM = lim2 if NORM else lim1

        sns.set(style='ticks', palette='Set2')
        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(9,3), constrained_layout=True, dpi=150)
        for ax_i, (ax, pair) in enumerate(zip(axes.flatten(), pairs)):
            scatterplot_maker.plot_pairwise_metrics(ax, concatenated, pair[0], pair[1], 
                                color_palette, titles_dict,
                                log=LOG, legend=False, fontsize=12, 
                                plot_lines=True, ylim=YLIM[ax_i])
        label = '_norm' if NORM else ''
        if LOG:
            plt.savefig(f'../data/{dataset}/scatterplots/{task_name}_log{label}.png', bbox_inches='tight')
        else:
            plt.savefig(f'../data/{dataset}/scatterplots/{task_name}{label}.png', bbox_inches='tight')

if __name__ == '__main__':
    main()