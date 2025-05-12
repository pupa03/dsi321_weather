import requests
import pandas as pd
from datetime import datetime
import time
import pytz
from datetime import timedelta

import nest_asyncio
import asyncio
import aiohttp
from prefect import flow, task
from math import ceil

from dotenv import load_dotenv
import os

nest_asyncio.apply()

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY")
SECRET_KEY = os.getenv("LAKEFS_SECRET_KEY")
lakefs_endpoint = os.getenv("LAKEFS_ENDPOINT", "http://lakefs-dev:8000")

@task
async def fetch_pollution_data(coord_df, dt, localtime, batch_size=300):
    POLLUTION_ENDPOINT = "http://api.openweathermap.org/data/2.5/air_pollution"

    async def fetch_row(session, row):
        # await asyncio.sleep(1)  # พัก 1 วิแบบไม่บล็อก loop
        lat = row['lat']
        lon = row['lon']
        province = row['province_en']
        district = row['district_en']
        district_id = row['district_id']
        try:
            params = {
                "lat" : lat,
                "lon" : lon,
                "appid": API_KEY,
                "units": "metric"
            }
            
            async with session.get(POLLUTION_ENDPOINT, params=params) as response:
                data = await response.json()

                components = data['list'][0]['components']
                pollution_dict = {
                    'timestamp': dt,
                    'year': dt.year,
                    'month': dt.month,
                    'day': dt.day,
                    'hour': dt.hour,
                    'minute': dt.minute,
                    'localtime': localtime,
                    'province' : province,
                    'district' : district,
                    'district_id' : district_id,
                    'lat' : data['coord']['lat'],
                    'lon' : data['coord']['lon'],
                    'main.aqi' : data['list'][0]['main']['aqi'],
                    'components_co' : components['co'],
                    'components_no' : components['no'],
                    'components_no2' : components['no2'],
                    'components_o3' : components['o3'],
                    'components_so2' : components['so2'],
                    'components_pm2_5' : components['pm2_5'],
                    'components_pm10' : components['pm10'],
                    'components_nh3' : components['nh3']
                    }
                return pollution_dict

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except KeyError as e:
            print(f"Error processing data: Missing key {e}")
            return None
        except Exception as e:
            print(f"Error: {e} at {province} - {district}")
            return None

    pollution_results = []
    total_batches = ceil(len(coord_df) / batch_size)  #sample
    async with aiohttp.ClientSession() as session:
        for i in range(total_batches):
            batch = coord_df.iloc[i*batch_size:(i+1)*batch_size]
            tasks = [fetch_row(session, row) for _, row in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            pollution_results.extend(batch_results)

            print(f"✅ เสร็จ batch {i+1}/{total_batches}")
            if i < total_batches - 1:
                await asyncio.sleep(65)  # รอให้ผ่าน rate limit

    return pollution_results


@task
async def fetch_weather_data(coord_df, dt, localtime, batch_size=300):  #sample
    WEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"
    
    async def fetch_row(session, row):
        # await asyncio.sleep(1)  # พัก 1 วิแบบไม่บล็อก loop
        lat = row['lat']
        lon = row['lon']
        province = row['province_en']
        district = row['district_en']
        district_id = row['district_id']
        try:
            params = {
                "lat" : lat,
                "lon" : lon,
                "appid": API_KEY,
                "units": "metric"
            }
            async with session.get(WEATHER_ENDPOINT, params=params) as response:
                data = await response.json()

                weather_dict = {
                    'timestamp': dt,
                    'year': dt.year,
                    'month': dt.month,
                    'day': dt.day,
                    'hour': dt.hour,
                    'minute': dt.minute,
                    'localtime': localtime,
                    'province' : province,
                    'district' : district,
                    'district_id' : district_id,
                    'lat' : data['coord']['lat'],
                    'lon' : data['coord']['lon'],
                    'weather_main' : data["weather"][0]["main"],
                    'weather_description' : data["weather"][0]["description"],
                    'main.temp' : data["main"]["temp"],
                    'main.temp_min' : data["main"]["temp_min"],
                    'main.temp_max' : data["main"]["temp_max"],
                    'main.feels_like' : data["main"]["feels_like"],
                    'main.pressure' : data["main"]["pressure"],
                    'main.humidity' : data["main"]["humidity"],
                    'visibility' : data.get("visibility"),
                    'wind.speed' : data["wind"]["speed"],
                    'wind.deg' : data["wind"]["deg"]
                    }
                return weather_dict
       
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None
        except KeyError as e:
            print(f"Error processing data: Missing key {e}")
            return None
        except Exception as e:
            print(f"Error: {e} at {province} - {district}")
            return None

    weather_results = []
    total_batches = ceil(len(coord_df) / batch_size)  #sample
    async with aiohttp.ClientSession() as session:
        for i in range(total_batches):
            batch = coord_df.iloc[i*batch_size:(i+1)*batch_size]
            tasks = [fetch_row(session, row) for _, row in batch.iterrows()]
            batch_results = await asyncio.gather(*tasks)
            weather_results.extend(batch_results)

            print(f"✅ เสร็จ batch {i+1}/{total_batches}")
            if i < total_batches - 1:
                await asyncio.sleep(65)  # รอให้ผ่าน rate limit

    return weather_results


def clean_data(df):
    df = pd.DataFrame(df)
    
    df['province'] = df['province'].astype("string")
    df['district'] = df['district'].astype("string")
    return df


def save_to_lakefs_pollution(df):
    repo = "pollution-data"
    branch = "main"
    path = "pollution.parquet"
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {"endpoint_url": lakefs_endpoint}
    }

    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=["year", "month", "day", "hour"],
    )


def save_to_lakefs_weather(df):
    repo = "weather-data"
    branch = "main"
    path = "weather.parquet"
    lakefs_s3_path = f"s3a://{repo}/{branch}/{path}"

    storage_options = {
        "key": ACCESS_KEY,
        "secret": SECRET_KEY,
        "client_kwargs": {"endpoint_url": lakefs_endpoint}
    }

    df.to_parquet(
        lakefs_s3_path,
        storage_options=storage_options,
        partition_cols=["year", "month", "day", "hour"],
    )


@flow(name="main-flow", log_prints=True)
async def main_flow():
    start_time = time.perf_counter()  # จับเวลา

    dt = datetime.utcnow()
    thai_tz = pytz.timezone('Asia/Bangkok')
    localtime = dt.astimezone(thai_tz)
    
    BASE_DIR = os.path.abspath(os.path.join(os.getcwd(), ".."))  #/home/jovyan/work
    coord_path = os.path.join(BASE_DIR, "save", "district_coord.csv")
    coord_df = pd.read_csv(coord_path)
    # df_sample = coord_df.sample(10)
    
    pollution_results = await fetch_pollution_data(coord_df, dt, localtime)
    weather_results = await fetch_weather_data(coord_df, dt, localtime)
    
    pollution_data = clean_data(pollution_results)
    weather_data = clean_data(weather_results)
    
    
    end_time = time.perf_counter()  # จับเวลาอีกครั้ง
    print(f"\n✅ ดึงข้อมูลเสร็จทั้งหมด ใช้เวลา {end_time - start_time:.2f} วินาที")
    
    save_to_lakefs_pollution(pollution_data)
    save_to_lakefs_weather(weather_data)
    print("save to Lakefs Success")
    # print(pollution_data.head())
    # print(weather_data.head())

