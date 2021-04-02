import json
import os

import dask.dataframe as dd


class ParquetLoader:
    def __init__(self, directory):
        directory = os.path.abspath(directory)
        self.events_df = []
        self.transactions_df = {}
        # load all json files
        files = [name for name in os.listdir(directory) if os.path.splitext(name)[-1] == ".json"]
        for filename in files:
            with open(os.path.join(directory, filename)) as f:
                config = json.load(f)
                if "type" not in config or "parquet" not in config or "name" not in config:
                    continue
                parquet_filename = os.path.join(directory, config["parquet"])
                if not os.path.exists(parquet_filename):
                    continue
                if not config["name"]:
                    raise ValueError("Name cannot be empty")
                name = config["name"]
                df_type = config["type"]
                # load the parquet file
                if df_type == "event":
                    ddf = dd.read_parquet(parquet_filename, index=["id", "time"])
                    self.events_df.append(ddf)
                else:
                    ddf = dd.read_parquet(parquet_filename, index=["id"])
                    self.transactions_df[name] = ddf
