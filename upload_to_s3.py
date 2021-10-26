import os
import csv
import time
import s3fs 
import zarr
import shutil
import argparse
from pathlib import Path

strategy = 'hybrid_time100_lat100_lon010'
print(strategy)

# backup_path = f'/backup/dieumynguyen/geos-fp-global__inst/rechunked/{strategy}'
efs_path = f'/efs/dieumynguyen/geos-fp-global__inst/rechunked/{strategy}'

# print('Copying from /backup to /efs')
# shutil.copytree(backup_path, efs_path)

s3_target = f'eis-dh-fire/dieumynguyen_rechunked/geos-fp-global__inst/{strategy}/'

print("Copying results to S3")
s3 = s3fs.S3FileSystem(anon=False) # API: Access S3 as if it were a file system
s3.put(str(efs_path), f"s3://{s3_target}", recursive=True)

print("Done!")



