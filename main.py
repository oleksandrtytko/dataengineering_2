import zipfile
from os import listdir

import aiofiles
import aiohttp
import asyncio

import requests
import os
from zipfile import ZipFile

downloads_path = 'downloads'

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

async_download = 0


def download_file_path(uri):
    return downloads_path + '/' + uri.split('/')[-1]


async def download(uri, session):
    async with session.get(uri) as response:
        if response.status == 200:
            async with aiofiles.open(download_file_path(uri), "wb") as f:
                await f.write(await response.read())


async def download_all_async():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *[download(item_id, session) for item_id in download_uris]
        )


def download_all_sync():
    for uri in download_uris:
        zip_path = download_file_path(uri)
        try:
            req = requests.get(uri)
            if req.status_code == 200:
                with open(zip_path, 'wb') as output_file:
                    output_file.write(req.content)
        except requests.exceptions.RequestException as e:
            print(e)


def main():
    if not os.path.exists(downloads_path):
        os.makedirs(downloads_path)

    if async_download:
        asyncio.run(download_all_async())
    else:
        download_all_sync()

    for file in listdir(downloads_path):
        zip_path = downloads_path + '/' + file
        try:
            with ZipFile(zip_path, 'r') as zObject:
                zObject.extractall(path=downloads_path)
        except zipfile.BadZipFile as e:
            print(e)
        os.remove(zip_path)
    pass


if __name__ == "__main__":
    main()
