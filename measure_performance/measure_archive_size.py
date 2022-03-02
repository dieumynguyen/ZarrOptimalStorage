import sys
import boto3
import pandas as pd

def get_path_size(bucket, path):
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket)
    total_size = 0

    for obj in my_bucket.objects.filter(Prefix=path):
        total_size = total_size + obj.size

    return total_size

def main():
    bucket = 'eis-dh-fire'
    dataset = 'geos-fp-global'
    archive = 'inst'
    variable = 'BCEXTTAU'

    folder = f'dieumynguyen_rechunked/{dataset}_{archive}/'
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=folder, Delimiter='/')

    # Get strategy names
    all_strategies = []
    for prefix in result.get('CommonPrefixes'):
        folder_path = prefix.get('Prefix')
        all_strategies.append(folder_path)

    print(f'Number of chunking strategies: {len(all_strategies)}')

    archive_sizes = {
        'time_chunks':    [],
        'lon_chunks':     [],
        'lat_chunks':     [],
        'archive_size':   []
    }

    for strat_i, strat in enumerate(all_strategies, start=0):
        try:
            # Process file name
            strategy_str = strat.split('/')[-2] 
            category = '_'.join(strategy_str.split('_')[:-3])
            strategy = '_'.join(strategy_str.split('_')[-3:])
            time_chunk = strategy.split('_')[0].split('time')[-1]
            lat_chunk = strategy.split('_')[1].split('lat')[-1]
            lon_chunk = strategy.split('_')[2].split('lon')[-1]

            sys.stdout.write(f'\rGetting size of strategy {strat_i} / {len(all_strategies)-1} -- {strategy}')
            sys.stdout.flush()

            # Get folder size in bytes
            total_size = get_path_size(bucket, strat)

            archive_sizes['time_chunks'].append(time_chunk)
            archive_sizes['lon_chunks'].append(lon_chunk)
            archive_sizes['lat_chunks'].append(lat_chunk)
            archive_sizes['archive_size'].append(total_size)
        except:
            continue
        
    # Create and save csv
    archive_sizes_df = pd.DataFrame(archive_sizes)
    archive_sizes_df.to_csv(f'../data/{dataset}_{archive}/performance_data/archive_sizes.csv')

if __name__ == '__main__':
    main()