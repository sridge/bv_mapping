import os
from glob import glob

import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy import create_engine


def property_to_zip(gids):

    user = os.environ.get("PRE_DB_USER")
    host = os.environ.get("PRE_DB_HOST")
    pw = os.environ.get("PRE_DB_PASS")
    db_name = "geo"
    engine = create_engine(f"postgresql://{user}@{host}:{pw}@{host}/{db_name}")

    df = pd.read_csv("/Users/sean/Desktop/zipcodes.csv")
    print("-------- unprocessed properties --------")
    print(len(df["gid"][df["gid"].isin(gids)]))
    print("----------------------------------------")

    for gid in gids:

        query = f"""
        WITH tem AS ( SELECT gid, zcta5ce10, geom FROM PUBLIC.us_zip WHERE gid = {gid} )
        SELECT
            tem.gid,
            zcta5ce10,
            AVG ( (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float ),
            COUNT ( CASE WHEN (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float < 0.6 THEN 1 END ) AS score_1,
            COUNT ( CASE WHEN (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float >= 0.6 AND (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float < 0.74 THEN 1 END ) AS score_2,
            COUNT ( CASE WHEN (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float >= 0.74 AND (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float < 0.83 THEN 1 END ) AS score_3,
            COUNT ( CASE WHEN (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float >= 0.83 AND (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float < 0.94 THEN 1 END ) AS score_4,
            COUNT ( CASE WHEN (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float >= 0.94 AND (pre_blue.footprint_geo_complete.predictions -> 'score' ->> 'value')::float <= 1 THEN 1 END ) AS score_5
        FROM
            pre_blue.footprint_geo_complete
            JOIN tem ON st_intersects ( pre_blue.footprint_geo_complete.geom, tem.geom )
        GROUP BY
            tem.gid,
            zcta5ce10
        """

        df_zipcode = pd.read_sql_query(query, engine, index_col="gid")

        zipcode = df["zcta5ce10"][df["gid"] == gid].values[0]
        df_zipcode.to_csv(f"./data/{gid}_{zipcode}.csv")


def cleanup_zip_region(zip_region):
    filenames = glob(f"./data/*_{zip_region}*")

    rerun = []
    for filename in filenames:
        with open(filename) as f:
            read_data = f.readlines()
        if len(read_data) < 2:
            rerun += [int(filename[7:].split("_")[0])]
    len(rerun)

    property_to_zip(rerun)


if __name__ == "__main__":

    zip_regions = [37, 38, 40, 41, 42, 63, 64, 65, 73, 74]
    # [58,57,80,81,85,86,87,88,89]
    Parallel(n_jobs=8, verbose=10)(delayed(cleanup_zip_region)(zip_region) for zip_region in zip_regions)
