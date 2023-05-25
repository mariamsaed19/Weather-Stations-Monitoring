from elasticsearch import Elasticsearch
import pandas as pd
import os

# # Connect to Elasticsearch
es = Elasticsearch(["http://localhost:9200"])

index_settings = {
    "mappings": {
        "properties": {
            "station_id": {"type": "long"},
            "s_no": {"type": "long"},
            "battery_status": {"type": "keyword"},
            "status_timestamp": {"type": "date"},
            "weather": {
                "properties": {
                    "humidity": {"type": "integer"},
                    "temperature": {"type": "integer"},
                    "wind_speed": {"type": "integer"}
                }
            }
        }
    }
}

index_name = "weather_station_"


#Define root directory for Parquet files
root_dir = "/home/ali/Desktop/test/parquet"

# Loop over the ids in the root directory
for id_dir in os.listdir(root_dir):
    id_path = os.path.join(root_dir, id_dir)
    if not os.path.isdir(id_path):
        continue
    es.indices.create(index=index_name+str(id_dir), individual=index_settings)
    # Loop over the timestamps in the id directory
    for timestamp_dir in os.listdir(id_path):
        timestamp_path = os.path.join(id_path, timestamp_dir)
        if not os.path.isdir(timestamp_path):
            continue

        # Load the Parquet files into a pandas DataFrame
        dfs = []
        for file_name in os.listdir(timestamp_path):
            if file_name.endswith(".parquet"):
                file_path = os.path.join(timestamp_path, file_name)
                dfs.append(pd.read_parquet(file_path))
        df = pd.concat(dfs)

        # Convert DataFrame to a list of dictionaries (one dictionary per row)
        data = df.to_dict("records")

        # Use the Elasticsearch index API to load the data
        for d in data:
            es.index(index=index_name+str(id_dir), document=d)



