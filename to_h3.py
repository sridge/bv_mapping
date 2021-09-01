from h3 import h3
import pandas as pd


def add_h3(filein: str, h3_level: int = 6) -> pd.DataFrame:
    """load a csv file and add H3 spatial index. Write to JSON if desired"""

    def lat_lng_to_h3(row):
        return h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)

    df = pd.read_csv(filein)
    df["hex"] = df.apply(lat_lng_to_h3, axis=1)

    return df


def count_h3(df, column_mapper: dict = {"property_id": "count"}):
    """count the number of rows in associated with a hex"""
    df_count = df.groupby(["hex"]).count()
    df_count = df_count.rename(columns=column_mapper)
    return df, df_count


def df_to_deck_json(df, fileout):
    """save the datagrame as a deck.gl friendly JSON"""
    df[["count"]].reset_index().to_json(fileout, orient="records")


if __name__ == "__main__":

    filein = "/Users/Sean/Desktop/tarp_area.csv"
    fileout = "tarp_areas.json"
    h3_level = 6

    df = add_h3(filein, h3_level=h3_level)
    df, df_count = count_h3(df)
    df_to_deck_json(df_count, fileout)
