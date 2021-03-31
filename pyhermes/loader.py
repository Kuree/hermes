import os
import json
import pyspark


class Loader:
    def __init__(self, directory):
        self.events_df = []
        self.transactions_df = None
        # load all json files
        self.spark = pyspark.sql.SparkSession.builder.appName("Hermes").getOrCreate()
        files = [name for name in os.listdir(directory) if
                 os.path.isfile(name) and os.path.splitext(name)[-1] == ".json"]
        for filename in files:
            with open(os.path.join(directory, filename)) as f:
                config = json.load(f)
                if "type" not in config or "parquet" not in config:
                    continue
                parquet_filename = os.path.join(directory, config["parquet"])
                if not os.path.exists(parquet_filename):
                    continue
                df_type = config["type"]
                # load the parquet file
                df = self.spark.read.parquet(parquet_filename)
                if df_type == "event":
                    self.events_df.append(df)
                else:
                    if self.transactions_df is None:
                        self.transactions_df = df
                    else:
                        # transactions has the same schema
                        self.transactions_df = self.transactions_df.union(df)
