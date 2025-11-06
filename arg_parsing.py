import argparse
import json
import requests
import sys
    
def send_data(endpoint,method,data=None,filter=None):
  url = f"https://jsonplaceholder.typicode.com/{endpoint}"
  try:
    if method == 'GET':
      if filter:
        url = url+f"/?{filter}"
      response = requests.get(url,timeout=10)
    elif method == 'POST':
      response = requests.post(url,data=data,timeout=10)
    elif method == 'PUT':
      response = requests.put(url,data=data,timeout=10)
    else:
      response = requests.delete(url,timeout=10)
    response.raise_for_status()
    return response.json()
    
  except requests.exceptions.Timeout:
    print("Request timed out")
  except requests.exceptions.HTTPError as e:
    print(f"HTTP error occured {e}")
  except Exception as e:
    print(f"An Error occured: {e}")
    sys.exit(1)


def parse_args():
  parser = argparse.ArgumentParser(description="basic API tool")
  parser.add_argument("endpoint",help="supported endpoints posts, users, comments")
  parser.add_argument("--limit","-l",type=int,required=False,help="limit the number of entries being shown")
  parser.add_argument("--filter","-f",type=str,required=False,help="filter based on a field value")
  parser.add_argument("--method","-m",choices=["GET","POST","PUT","DELETE"],default="GET",help="HTTP method to use (default: GET)")
  parser.add_argument("--data","-d",type=str,required=False,help="Pass the data to API")
  return parser.parse_args()

def display(data,limit=None):
  final_data = data[:limit] if limit else data
  print(json.dumps(final_data,indent=2))

def main():
  args = parse_args()
  result = {}
  try:
    if not args.data and args.method != 'GET' and args.method != 'DELETE':
      print("Need data for this operation")
      sys.exit(1)

    result = send_data(args.endpoint,args.method,args.data,args.filter)
    if not result:
      print("Query returned no results")
      sys.exit(0)
  
    display(result,args.limit)
  
  except Exception as e:
    print(f"Unexpected Error: {e}")
    sys.exit(1)
  

if __name__ == '__main__':
  main()