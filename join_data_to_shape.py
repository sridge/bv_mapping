import pandas as pd
import dask.dataframe as dd

from dask.distributed import Client, LocalCluster

# start dask_cluster (track progress at localhost:8787/status)
cluster = LocalCluster()
client = Client(cluster)


def insert_data_into_json(row):
    """update geojson type from geometry to Feature"""

    new_geojson = {}
    new_geojson["type"] = "Feature"
    new_geojson["geometry"] = eval(row["st_asgeojson"])

    tem = {}
    tem["zipcode"] = row["zcta5ce10"]
    for score_val in range(1, 6):
        tem[f"score_{score_val}"] = row[f"score_{score_val}"]

    tem["percentage_1"] = (
        (row[f"score_1"]) / (row[f"score_1"] + row[f"score_2"] + row[f"score_3"] + row[f"score_4"] + row[f"score_5"])
    ) * 100
    new_geojson["properties"] = tem

    row["st_asgeojson"] = new_geojson

    return row


if __name__ == "__main__":

    for zipcode_region in range(0, 10):

        print("----------------")
        print(f"creating geojson for zipcode region {zipcode_region}")
        print("----------------")

        # load data from every single zipcode in that region
        df = dd.read_csv(f"./data/*_{zipcode_region}*.csv").compute()

        # load shapes for a single zipcode region
        df_shape = pd.read_csv(f"./zipcodes/zipcode_shape_{zipcode_region}.csv")

        # join to that region and save intermediate result
        df = df.join(df_shape.set_index("gid"), on="gid")
        df = df.dropna()
        df = df.set_index("gid")
        df.to_parquet(f"joined_{zipcode_region}")

        # reformat to geojson and save intermediate result
        df = pd.read_parquet(f"joined_{zipcode_region}")
        df = df.apply(insert_data_into_json, axis=1)
        df.to_parquet(f"converted_{zipcode_region}")

        # save as geojson for that zipcode region
        df = pd.read_parquet(f"converted_{zipcode_region}")
        df["st_asgeojson"].to_json(f"zipcode_region_{zipcode_region}.json", orient="values")

        prefix = '{"type": "FeatureCollection", "features": '
        suffix = "}"

        with open(f"zipcode_region_{zipcode_region}.json", "r") as f:
            read_data = f.read()
        with open(f"zipcode_region_{zipcode_region}.json", "w") as f:
            write_data = prefix + read_data + suffix
            f.write(write_data)
