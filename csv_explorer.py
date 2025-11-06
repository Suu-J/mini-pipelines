import pandas as pd
import sys
import argparse
from pathlib import Path


def analyzer(csv_location, column, top, report_file):
  output_lines = []
  output_lines.append("\n CSV Explorer results")
  
  df = pd.read_csv(csv_location)
  
  output_lines.append("\n Printing all columns: ")
  output_lines.append(str(df.columns.tolist()))

  output_lines.append(f"\n Total row count: {len(df)}")

  if column not in df.columns:
    output_lines.append(f"Error: column: {column} not found in csv file")
    output_text = "\n".join(output_lines)
    print(output_text)
    if report_file:
      with open(report_file, "w", encoding="utf-8") as f:
        f.write(output_text)
    sys.exit(1)

  output_lines.append(f"\n Summary of column: {column}")
  output_lines.append(str(df[column].describe()))

  if top:
    output_lines.append(f"\n Printing top {top} rows")
    output_lines.append(str(df.head(top)))

  output_text = "\n".join(output_lines)
  print(output_text)
  if report_file:
    with open(report_file, "w", encoding="utf-8") as f:
      f.write(output_text)
    print("report file created")


def parse_args():
  parser = argparse.ArgumentParser(description="CSV Explorer Tool")
  parser.add_argument("csv_location", help="Path to CSV file")
  parser.add_argument("column", help="Column name to summarise")
  parser.add_argument("--top",
                      "-t",
                      type=int,
                      required=False,
                      help="--top n would output_lines.append top n rows")
  parser.add_argument(
      "--report",
      '-r',
      type=str,
      required=False,
      help="use --report with given file name to generate report")
  
  return parser.parse_args()


def main():
  args = parse_args()
  csv_location = Path(args.csv_location)
  analyzer(csv_location, args.column, args.top, args.report)


if __name__ == '__main__':
  main()
