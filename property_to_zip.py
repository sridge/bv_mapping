import os
from glob import glob
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine


user = os.environ.get("PRE_DB_USER")
host = os.environ.get("PRE_DB_HOST")
pw = os.environ.get("PRE_DB_PASS")
db_name = "geo"
engine = create_engine(f"postgresql://{user}@{host}:{pw}@{host}/{db_name}")

print("reading")
df = pd.read_csv("/Users/sean/Desktop/zipcodes.csv")
filenames = glob("./data/*.csv")
gids = [filename.split("_")[0][7:] for filename in filenames]
print("----------------------")
print(len(df["gid"]))
print("----------------------")
print(len(df["gid"][~df["gid"].isin(gids)]))
print("----------------------")


for gid in tqdm(df["gid"][~df["gid"].isin(gids)]):

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

    df_zipcode = pd.read_sql(query, engine, index_col="gid")

    zipcode = df["zcta5ce10"][df["gid"] == gid].values[0]
    df_zipcode.to_csv(f"./data/{gid}_{zipcode}.csv")
