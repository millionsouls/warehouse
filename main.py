import pandas as pd
import sqlite3
import argparse
import sys
import os
import tempfile
import zipfile
import requests

def load_from_file(filepath):
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        return pd.read_csv(filepath)
    elif ext == ".json":
        return pd.read_json(filepath)
    elif ext == ".zip":
        with zipfile.ZipFile(filepath, 'r') as z:
            for name in z.namelist():
                if name.endswith('.csv'):
                    with z.open(name) as f:
                        return pd.read_csv(f)
                elif name.endswith('.json'):
                    with z.open(name) as f:
                        return pd.read_json(f)
            print("No CSV or JSON file found in ZIP archive.")
            sys.exit(1)
    else:
        print(f"Unsupported file extension: {ext}")
        sys.exit(1)

def load_from_url(url):
    ext = os.path.splitext(url.split("?")[0])[1].lower()
    with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
        try:
            r = requests.get(url, stream=True)
            r.raise_for_status()
            for chunk in r.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.flush()
        except Exception as e:
            print(f"Failed to download file: {e}")
            sys.exit(1)
        return load_from_file(tmp.name)

def main():
    parser = argparse.ArgumentParser(description="Process file or URL into DB")
    parser.add_argument("source", nargs="?", help="Path to file or URL")
    args = parser.parse_args()

    if not args.source:
        args.source = input("Enter path to file or URL: ").strip()

    if args.source.startswith("http://") or args.source.startswith("https://"):
        df = load_from_url(args.source)
    else:
        if not os.path.isfile(args.source):
            print(f"File not found: {args.source}")
            sys.exit(1)
        df = load_from_file(args.source)

    # Optional: clean and standardize column names
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    if "arrest_date" in df.columns and "ofns_desc" in df.columns:
        df = df.dropna(subset=["arrest_date", "ofns_desc"])  # example filter

    # Connect to SQLite and write to DB
    conn = sqlite3.connect("warehouse.db")
    df.to_sql("arrests", conn, if_exists="replace", index=False)
    conn.close()

    print("Data loaded into warehouse.db")

if __name__ == "__main__":
    main()
