import argparse
import re
import json
from collections import Counter


def process_log(log_location):

  data = []

  with open(log_location, "r", encoding="utf-8") as f:

    for line in f:
      line_data = {
          "ip": '',
          "time": '',
          "http_method": '',
          "URL": '',
          "status": '',
          "response_size": ''
      }
      line = line.strip()

      ip = re.search(r"^\d+\.\d+\.\d+", line).group()
      line_data["ip"] = ip

      timestamp = re.search(r"\[.+\]", line).group()
      time_content = str(timestamp.lstrip("[").rstrip("]"))
      line_data["time"] = time_content

      http_method = re.search(r'\"\w+\s', line).group()
      http_content = str(http_method)[1:-1]
      line_data["http_method"] = http_content

      url_path = re.findall(r'"[A-Z]+ (\S+) HTTP/[0-9\.]+"', line)
      url_content = str(url_path)[2:-2]
      line_data["URL"] = url_content

      status = re.search(r"\s\d{3}\s", line).group()
      status_content = str(status).strip()
      line_data["status"] = status_content

      response_size = re.search(r"\d+$", line).group()
      line_data["response_size"] = response_size

      data.append(line_data)

  return data


def aggregation(data_dict):
  agg_result = {
      "total_lines_parsed": 0,
      "unique_ips": 0,
      "status_code_counts": [],
      "url_paths_counts": [],
      "http_methods_counts": []
  }
  agg_result["total_lines_parsed"] = len(data_dict)
  agg_result["status_code_counts"] = Counter(
      [line["status"] for line in data_dict])
  agg_result["unique_ips"] = len(set([line["ip"] for line in data_dict]))
  agg_result["url_paths_counts"] = Counter([line["URL"] for line in data_dict])
  agg_result["http_methods_counts"] = Counter(
      [line["http_method"] for line in data_dict])

  return agg_result


def fetch_status(status_to_get, data_dict):
  status_result = [
      row for row in data_dict if row["status"] == str(status_to_get)
  ]

  return status_result


def fetch_method(method_to_get, data_dict):
  method_result = [
      row for row in data_dict if row["http_method"] == method_to_get
  ]

  return method_result

def display(data_dict,limit=None):
  if limit and limit <= len(data_dict):
    for i in range(limit):
      print(data_dict[i])
  else:
    for row in data_dict:
      print(row)
      
def parse_args():
  parser = argparse.ArgumentParser(description="Log Parser Tool")
  parser.add_argument("log_location", help="location to the log file")
  parser.add_argument("--status",
                      "-s",
                      type=int,
                      required=False,
                      help="fetch only the mentioned kind of status code")
  parser.add_argument("--method",
                      "-m",
                      type=str,
                      required=False,
                      help="Fetch only the mentioned kind of HTTP method")
  parser.add_argument("--limit",
                      "-l",
                      type=int,
                      required=False,
                      help="Fetch first x number of entries")
  parser.add_argument("--summary",
                      required=False,
                      action="store_true",
                      help="print only aggregated summary")
  parser.add_argument("--raw",
                      required=False,
                      action="store_true",
                      help="Print raw data")

  return parser.parse_args()


def main():
  args = parse_args()

  result = process_log(args.log_location)

  # do below if summary flag is present
  if args.summary:
    aggregated_result = aggregation(result)
    json_output = json.dumps(aggregated_result, indent=4)
    print(json_output)
  elif args.raw:
    print(result)
  
  # Filtering optionally
  if args.status:
    print("printing rows with status: ", args.status)
    result_status = fetch_status(args.status, result)
    display(result_status,args.limit)

  if args.method:
    print("printing rows with methods: ", args.method)
    result_method = fetch_method(args.method, result)
    display(result_method,args.limit)

if __name__ == "__main__":
  main()
