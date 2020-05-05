from multiprocessing.pool import ThreadPool

import pandas as pd
import subprocess
import requests
import glob
import json
import tqdm
import time
import pdb
import os


postcodes_wy = pd.read_csv('postcodes/postcodes_wy.tsv', sep='\t', index_col=0)['area']

df = pd.DataFrame()
for borough_csv in glob.glob('postcodes/ONSPD_NOV_2019_UK_*.csv'):
    borough = pd.read_csv(borough_csv)
    borough['postcode_area'] = borough['pcds'].apply(lambda x: x.split(' ')[0])
    borough = borough[borough['postcode_area'].isin(postcodes_wy)]
    df = df.append(borough)

STREET_API_URL = "https://maps.googleapis.com/maps/api/streetview"
BASE_URL = 'https://maps.google.com'

def process(iterrow):
    direction, row = iterrow
    
    name = f"{row['pcd']}-{row['lsoa11']}".replace(' ', '_')
    filename = f"download_images_server/new_downloaded_images/{name}-{direction}.jpeg"
    if os.path.exists(filename):
        return True
    
    try:
        params = {
            'location': f"{row['lat']}, {row['long']}",
            'heading': direction,
            'key': os.environ['STREET_VIEW_STATIC_API_KEY']
        }
        response = json.loads(requests.get(f"{STREET_API_URL}/metadata", params=params).text)
        
        if response['status'] != 'OK':
            # print(f"Google Status not OK {response} {params['location']}")
            return False
        
        # lat = round(response['location']['lat'], 6)
        # lng = round(response['location']['lng'], 6)
        lat = row['lat']
        lng = row['long']
        
        main_url = f'{BASE_URL}/cbk'
        image = requests.get(main_url, stream=True, params={
            'output': 'thumbnail',
            'w': 640,
            'h': 640,
            'panoid': response['pano_id']
        })
        
        if image.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(image.content)
            return True
        else:
            return False
        return False
        
    except Exception:
        return False

# Asynchronous
def df_gen():
    for _, row in df.iterrows():
        for direction in ['0']:
            yield (direction, row)

with ThreadPool(30) as p:
        with tqdm.tqdm(total=1*df.shape[0]) as pbar:
            for i, _ in enumerate(p.imap_unordered(process, df_gen())):
                pbar.update()

# Synchronous
# for iterrow in tqdm.tqdm(df.iterrows(), total=df.shape[0]):
#     process(iterrow)