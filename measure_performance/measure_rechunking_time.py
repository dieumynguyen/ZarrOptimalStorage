import sys
import glob
import datetime
import calendar
import numpy as np
import pandas as pd

def month_str_to_num(month_str):
    month_num = list(calendar.month_abbr).index(month_str)
    return month_num

def extract_time(time_line):
    time_line = time_line.split(' ')
    year = int(time_line[-3])
    month = month_str_to_num(time_line[4])
    try:
        day = int(time_line[5])
        time = time_line[6].split(':')
    except:
        day = int(time_line[6])
        time = time_line[7].split(':')
    hour = int(time[0])
    minute = int(time[1])
    second = int(time[2])
    return (year, month, day, hour, minute, second)

def get_runtime(start_tuple, end_tuple):
    start_time = datetime.datetime(start_tuple[0], start_tuple[1], start_tuple[2], start_tuple[3], start_tuple[4], start_tuple[5])
    end_time = datetime.datetime(end_tuple[0], end_tuple[1], end_tuple[2], end_tuple[3], end_tuple[4], end_tuple[5])
    runtime_hours = abs(start_time - end_time).total_seconds() / 3600
    return runtime_hours

def main():
    dataset = 'geos-fp-global'
    archive = 'inst'
    folder = 'rechunking-logs'

    logs_path = f'../data/{dataset}_{archive}/{folder}'
    out_paths = np.sort(glob.glob(f'{logs_path}/*.out'))

    runtimes = {
    'time_chunks':    [],
    'lon_chunks':     [],
    'lat_chunks':     [],
    'runtime_hr':     []
    }

    for out_i, out_path in enumerate(out_paths):
        sys.stdout.write(f'\rGetting runtime of strategy {out_i} / {len(out_paths)-1}')
        sys.stdout.flush()
        
        # Process file name
        strategy_str = out_path.split('/')[-1].split('.')[0].split('_')[3:]
        category = '_'.join(strategy_str[:-3])
        strategy = '_'.join(strategy_str[-3:])
        time_chunk = strategy.split('_')[0].split('time')[-1]
        lat_chunk = strategy.split('_')[1].split('lat')[-1]
        lon_chunk = strategy.split('_')[2].split('lon')[-1]
        
        with open(out_path) as f:
            lines = f.readlines()
            
        start = lines[0]
        end = lines[-1]
        
        if end == 'Copying results to S3\n':
            complete_line = [ele for ele in lines if "100%" in ele][0]
            final_time = complete_line.split(" | ")[-1].strip()
            final_time_split = final_time.split(' ')
            final_time_split = [x for x in final_time_split if x]
            hour = final_time_split[0].split('hr')[0]
            minute = final_time_split[1] 
            minute = minute.split('min')[0]
            runtime_hours = int(hour) + (int(minute)/60)
        else:
            start_tuple = extract_time(start)
            end_tuple = extract_time(end)
            runtime_hours = get_runtime(start_tuple, end_tuple)

        runtimes['time_chunks'].append(time_chunk)
        runtimes['lon_chunks'].append(lon_chunk)
        runtimes['lat_chunks'].append(lat_chunk)
        runtimes['runtime_hr'].append(runtime_hours)

    # Create and save csv
    runtime_df = pd.DataFrame(runtimes)
    runtime_df.to_csv(f'../data/{dataset}_{archive}/performance_data/rechunking_time.csv')

if __name__ == '__main__':
    main()