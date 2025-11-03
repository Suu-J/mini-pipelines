from dotenv import load_dotenv
import requests
import os
import sys
from datetime import datetime
import pandas as pd
from typing import cast
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

load_dotenv()
API_KEY = os.environ.get("API_KEY")
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
S3_OBJECT_KEY = os.environ.get("S3_OBJECT_KEY")

DATA_DIR = cast(
    str, os.getenv("DATA_DIR")
)  # we are casting here cuz it could return None type if DATA_DIR doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)
LOCAL_FILE_PATH = os.path.join(DATA_DIR, "weather_data.csv")

def extract(URL):
  try:
    response = requests.get(URL, timeout=10)
    response.raise_for_status()
    return response.json()

  except requests.exceptions.Timeout:
    print("Request timed out")
  except requests.exceptions.HTTPError as e:
    print(f"HTTP error occured {e}")
  except Exception as e:
    print(f"An Error occured: {e}")
    sys.exit(1)

def display(results):
  for row in results:
    print(
        f"{row['id']},{row['name']},{row['main']['temp']},{row['main']['pressure']},{row['main']['humidity']},{datetime.now()}"
    )

def save_as_csv(results):
  rows = []
  for data in results:
    rows.append({
        "id": data["id"],
        "name": data["name"],
        "temp": data["main"]["temp"],
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "timestamp": datetime.now()
    })
  df = pd.DataFrame(rows)
  try:
    df.to_csv(LOCAL_FILE_PATH, index=False)
  except Exception as e:
    print(f"An Error occured: {e}")
    sys.exit(1)


def upload_to_s3():
  try:
    s3 = boto3.client(service_name='s3',
      region_name=AWS_REGION,
      aws_access_key_id=AWS_ACCESS_KEY,
      aws_secret_access_key=AWS_SECRET_KEY)

    s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_OBJECT_KEY)
    print("Uploaded to s3 Location")
    
  except FileNotFoundError:
    print("The file was not found")
  except NoCredentialsError:
    print("AWS credentials not available")
  except ClientError as e:
    print(f"AWS Client error: {e}")
  except Exception as e:
    print(f"An Exception occured {e}")
    

def main():
  results = []
  lat = [51.50, 34.33, 12.78, 45.98, 18.91]
  lon = [00.12, 78.99, 32.67, 29.65, 72.83]

  try:
    for lati, long in zip(lat, lon):
      URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lati}&lon={long}&appid={API_KEY}"
      data = extract(URL)
  
      if not data:
        print("API returned no results")
        sys.exit(0)
  
      results.append(data)
  
    display(results)
    save_as_csv(results)
    upload_to_s3()

  except Exception as e:
    print(f"Error occured in Main: {e}")

if __name__ == "__main__":
  main()
