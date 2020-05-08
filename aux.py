import glob
from PIL import Image
import numpy as np
import tqdm
import pdb
import os

base_folder = 'download_images_server'

filenames = set([
    os.path.splitext(os.path.basename(f))[0][:-2]
    for f in glob.glob('download_images_server/360_downloaded_images/*')
    ])

for filename in tqdm.tqdm(filenames):
    pcd, lsoa, x = filename.split('-')
    final_path = f"{base_folder}/720_downloaded_images/{pcd}-{lsoa}-{int(x)*90}.jpeg"
    if os.path.exists(final_path):
        continue
    try:
        img0 = np.array(Image.open(f"{base_folder}/360_downloaded_images/{pcd}-{lsoa}-{x}-0.jpeg"))
        img1 = np.array(Image.open(f"{base_folder}/360_downloaded_images/{pcd}-{lsoa}-{x}-1.jpeg"))
        Image.fromarray(np.concatenate((img0, img1))[256:256+512, :, :]).save(final_path)
    except OSError:
        continue