from multiprocessing.pool import ThreadPool
from PIL import Image

import pandas as pd
import numpy as np
import subprocess
import requests
import random
import glob
import json
import tqdm
import time
import pdb
import os
import io


STREET_API_URL = "https://maps.googleapis.com/maps/api/streetview"

def process(iterrow):
    direction, row = iterrow
    
    name = f"{row['pcd']}-{row['lsoa11']}".replace(' ', '_')
    filename = f"download_images_server/360_downloaded_images/{name}-{direction}.jpeg"
    if os.path.exists(filename):
        return True
    
    try:
        params = {
            'location': f"{row['lat']}, {row['long']}",
            'key': os.environ['STREET_VIEW_STATIC_API_KEY']
        }
        response = json.loads(requests.get(f"{STREET_API_URL}/metadata", params=params).text)
        
        if response['status'] != 'OK':
            return False
        
        main_url = f'https://geo{random.randint(0, 3)}.ggpht.com/cbk'
        
        images = [None, None]
        for y in [0, 1]:
            image = requests.get(main_url, timeout=15, params={
                'cb_client': 'maps_sv.tactile',
                'authuser': 0,
                'hl': 'en',
                'gl': 'br',
                'x': direction // 90,
                'y': y,
                'zoom': 2,
                'nbt': None,
                'fover': 0,
                'output': 'tile',
                'panoid': response['pano_id']
            })
        
            if image.status_code == 200:
                images[y] = np.array(Image.open(io.BytesIO(image.content)))
            else:
                return False
        Image.fromarray(np.concatenate(tuple(images))[256:256+512, :, :]).save(filename)
        return True  
    except Exception:
        return False

# Asynchronous
def df_gen():
    for chunk in pd.read_csv('postcodes/ONSPD_NOV_2019_UK_London.csv', chunksize=1e6):
        for _, row in chunk.iterrows():
            for dir in [0, 90, 180, 270]:
                yield (dir, row)

with ThreadPool(30) as p:
        with tqdm.tqdm(total=8*321375) as pbar:
            for i, _ in enumerate(p.imap_unordered(process, df_gen())):
                pbar.update()

# Synchronous
# for iterrow in tqdm.tqdm(df.iterrows(), total=df.shape[0]):
#     process(iterrow)
