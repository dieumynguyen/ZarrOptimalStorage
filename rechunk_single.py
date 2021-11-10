import os
import csv
import time
import s3fs 
import zarr
import shutil
import argparse
import rechunker
import xarray as xr
from pathlib import Path
from dask.diagnostics import ProgressBar

def setup_args():
    parser = argparse.ArgumentParser(description="Generic rechunking script")
    parser.add_argument('--strat_description', type=str, help='Kind of chunking strategy, e.g. "hybrid" ') 
    parser.add_argument('--input_path', type=str, help='Input path on S3') # If no default, default is None  # 'eis-dh-fire/imerg-fwi.zarr'
    parser.add_argument('--output_path', type=str, help='Output path on S3')
    parser.add_argument('--tmp_path', type=str, help='Temporary path')
    parser.add_argument('--timevar', type=str, help='Name of time variable', default='time')
    parser.add_argument('--xvar', type=str, help='Name of x variable', default='lon')
    parser.add_argument('--yvar', type=str, help='Name of y variable', default='lat')
    parser.add_argument('--timechunk', type=int, help='Chunks for time variable', default=10)
    parser.add_argument('--xchunk', type=int, help='Chunks for x variable', default=10)
    parser.add_argument('--ychunk', type=int, help='Chunks for y variable', default=10)
    parser.add_argument('--data_variable', type=str, help='Variable to select from dataset', default=None)
    return parser.parse_args()

def setup_output_path(path, strategy, base_name, dataset_name, folder_name, output_type):
    if path is None:
        path = Path(f'/efs/dieumynguyen/{dataset_name}/{folder_name}/{strategy}') / base_name
        if path.is_dir():
            print('Cleaning up old dir.')
            shutil.rmtree(str(path))  # Clean up
        path.parent.mkdir(exist_ok=True, parents=True)
    return path

def create_dirs(args):
    # if args.timechunk == 'all' and args.xchunk == 'all' and args.ychunk == 'all':
    #     strategy = f'{args.strat_description}_time{args.timechunk}_lat{args.ychunk}_lon{args.xchunk}'
    # elif args.timechunk == 'all' and args.xchunk != 'all' and args.ychunk != 'all':
    #     strategy = f'{args.strat_description}_time{args.timechunk}_lat{args.ychunk:03d}_lon{args.xchunk:03d}'
    # elif args.timechunk != 'all' and args.xchunk == 'all' and args.ychunk == 'all':
    #     strategy = f'{args.strat_description}_time{args.timechunk:03d}_lat{args.ychunk}_lon{args.xchunk}'
    # elif args.timechunk != 'all' and args.xchunk == 'all' and args.ychunk != 'all':  # x = lon
    #     strategy = f'{args.strat_description}_time{args.timechunk:03d}_lat{args.ychunk:03d}_lon{args.xchunk}'
    # elif args.timechunk != 'all' and args.xchunk != 'all' and args.ychunk == 'all':  # y = lat
    #     strategy = f'{args.strat_description}_time{args.timechunk:03d}_lat{args.ychunk}_lon{args.xchunk:03d}'
    # else:
    #     strategy = f'{args.strat_description}_time{args.timechunk:03d}_lat{args.ychunk:03d}_lon{args.xchunk:03d}'

    if args.timechunk == 'all' and args.xchunk == 'all' and args.ychunk == 'all':
        strategy = f'time{args.timechunk}_lat{args.ychunk}_lon{args.xchunk}'
    elif args.timechunk == 'all' and args.xchunk != 'all' and args.ychunk != 'all':
        strategy = f'time{args.timechunk}_lat{args.ychunk:04d}_lon{args.xchunk:04d}'
    elif args.timechunk != 'all' and args.xchunk == 'all' and args.ychunk == 'all':
        strategy = f'time{args.timechunk:04d}_lat{args.ychunk}_lon{args.xchunk}'
    elif args.timechunk != 'all' and args.xchunk == 'all' and args.ychunk != 'all':  # x = lon
        strategy = f'time{args.timechunk:04d}_lat{args.ychunk:04d}_lon{args.xchunk}'
    elif args.timechunk != 'all' and args.xchunk != 'all' and args.ychunk == 'all':  # y = lat
        strategy = f'time{args.timechunk:04d}_lat{args.ychunk}_lon{args.xchunk:04d}'
    else:
        strategy = f'time{args.timechunk:04d}_lat{args.ychunk:04d}_lon{args.xchunk:04d}'

    # Get base dir name
    base_name = Path(args.input_path).name   # Truncate to only file part
    if args.input_path == 'eis-dh-fire/imerg-fwi.zarr':
        dataset_name = f"{base_name.split('.')[0]}"
    else:
        dataset_name = f"{args.input_path.split('/')[1]}_{base_name.split('.')[0]}"

    # Output path
    output_path = args.output_path
    output_path = setup_output_path(output_path, strategy, base_name, dataset_name,'rechunked', 'final output')

    # Temp path
    tmp_path = args.tmp_path
    tmp_path = setup_output_path(tmp_path, strategy, base_name, dataset_name, 'tmp-rechunk', 'temp output')

    # Create s3 path
    s3_target = Path(f'eis-dh-fire/dieumynguyen_rechunked/{dataset_name}/{strategy}') / base_name

    return output_path, tmp_path, strategy, base_name, s3_target

def convert_all(var):
    if var == 999:
        var = 'all'
    return var

def main(args):
    args.timechunk = convert_all(args.timechunk)
    args.xchunk = convert_all(args.xchunk)
    args.ychunk = convert_all(args.ychunk)
    print(args.timechunk, args.xchunk, args.ychunk)

    # Create output dirs
    output_path, tmp_path, strategy, base_name, s3_target = create_dirs(args)

    print(f"Reading data from S3: {args.input_path}")
    print(f'Strategy: timechunk: {args.timechunk}, lat/y-chunk: {args.ychunk}, lon/x-chunk: {args.xchunk}')
    print(f'Storing final output in: {output_path}')
    print(f'Storing temp output in: {tmp_path}')
    print(f'S3 target path: {s3_target}')

    # Load and open input file
    s3 = s3fs.S3FileSystem(anon=False) # API: Access S3 as if it were a file system
    assert s3.exists(args.input_path), f"Input path {args.input_path} not found on S3"

    # Create key/value store based on this file-system
    input_s3 = s3.get_mapper(args.input_path)

    # Load and decode a dataset from a Zarr store
    input_data = xr.open_zarr(input_s3, consolidated=True).unify_chunks()  # Return dataset: multi-dimensional, in memory, array database

    # Select only a variable if applicable 
    if args.data_variable != 'None': 
        print(f'Selecting variable: {args.data_variable}')
        input_data = input_data[[args.data_variable]]

    # Get current chunks
    current_chunks = input_data.chunks   # chunked by 'time' by default?
    ntime = len(current_chunks["time"])
    nlat = len(current_chunks["lat"])
    nlon = len(current_chunks["lon"])
    print(f'Number of current time chunks: {ntime} -- lat chunks: {nlat} -- lon chunks: {nlon}')
    
    # Get <all> values
    all_time = sum(current_chunks["time"])
    all_lat = sum(current_chunks["lat"])
    all_lon = sum(current_chunks["lon"])
    print(f'Number of all time chunks: {all_time} -- all lat chunks: {all_lat} -- all lon chunks: {all_lon}')

   # Make dict of assigned arguments
    new_chunks = {
        args.timevar: all_time if args.timechunk=='all' else args.timechunk,
        args.xvar: all_lon if args.xchunk=='all' else args.xchunk,
        args.yvar: all_lat if args.ychunk=='all' else args.ychunk
    }

    # Another dict to assign new_chunks as key to each input_data key
    # e.g. {'acet.biomass': {'time': 100, 'lon': 50, 'lat': 50}, 'acet.biomass_gl': {'time': 100, 'lon': 50, 'lat': 50}}
    new_chunks2 = {
        **{k: new_chunks for k in input_data.keys()},
        args.timevar: None,
        args.xvar: None,
        args.yvar: None
    }
    print(new_chunks2)

    print("Preparing rechunker...")
    rechunked = rechunker.rechunk(
        input_data,
        max_mem='100GiB',
        target_chunks=new_chunks2,
        target_store=str(output_path),
        temp_store=str(tmp_path)
    )

    print("Executing rechunker...")
    with ProgressBar():
        rechunked.execute()

    print("Consolidating metadata...")
    zarr.consolidate_metadata(str(output_path))

    # print("Copying results to S3")
    # s3.put(str(output_path), f"s3://{s3_target}", recursive=True)

    # print("Removing data from /efs/")
    # shutil.rmtree(str(output_path))
    # shutil.rmtree(str(tmp_path))

    print("Done!")

if __name__ == '__main__':
    args = setup_args()
    main(args)