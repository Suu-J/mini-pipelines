import requests
from dotenv import load_dotenv
import os
import sys
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

load_dotenv()
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
S3_OBJECT_KEY = os.environ.get("S3_OBJECT_KEY")
LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH")


def extract(url):
  try:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()

  except requests.exceptions.Timeout:
    print("Request timed out")
  except requests.exceptions.HTTPError as e:
    print(f"HTTP error occured {e}")
  except Exception as e:
    print(f"An Error occured: {e}")
    sys.exit(1)


def display(result):
  for row in result:
    coin = row['id']
    price = row['current_price']
    change = round(row['price_change_percentage_24h'], 2)
    print(f"{coin} | {price} | {change}%")


def save_as_csv(result):
  df = pd.DataFrame(result)
  df['ingestion_timestamp'] = pd.Timestamp.now()
  df.to_csv(LOCAL_FILE_PATH,
            columns=[
                'id', 'symbol', 'name', 'current_price', 'market_cap',
                'total_volume', 'price_change_percentage_24h',
                'ingestion_timestamp'
            ],
            index=False)


def upload_to_s3():
  try:
    s3 = boto3.client(service_name='s3',
                      region_name=AWS_REGION,
                      aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY)

    s3.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_OBJECT_KEY)

    print("Uploaded to S3 location")

  except FileNotFoundError:
    print("The file was not found")
  except NoCredentialsError:
    print("AWS credentials not available")
  except ClientError as e:
    print(f"AWS Client error: {e}")
  except Exception as e:
    print(f"An Exception occured {e}")


def main():
  try:
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1"
    result = extract(url)

    if not result:
      print("Query returned no results")
      sys.exit(0)

    # display(result)

    save_as_csv(result)
    upload_to_s3()

  except Exception as e:
    print(f"An Error occured: {e}")
    sys.exit(1)


if __name__ == '__main__':
  main()
