import pandas as pd
import os
import sys
import zipfile
import tempfile
import requests

class Loader:
    @staticmethod
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

    @staticmethod
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
            return Loader.load_from_file(tmp.name)