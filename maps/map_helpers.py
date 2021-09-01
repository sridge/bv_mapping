from h3 import h3
import pandas as pd
import matplotlib.pyplot as plt


def add_h3(filein: str, h3_level: int = 6, lat_col: str = "latitude", lon_col: str = "longitude") -> pd.DataFrame:
    """load a csv file and add H3 spatial index. Write to JSON if desired"""

    def lat_lng_to_h3(row):
        return h3.geo_to_h3(row["latitude"], row["longitude"], h3_level)

    df = pd.read_csv(filein)

    if lat_col != "latitude":
        df = df.rename(columns={lat_col: "latitude"})
    if lon_col != "longitude":
        df = df.rename(columns={lon_col: "longitude"})

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


def max_min_scaler(series: pd.Series) -> pd.Series:
    return (series - series.min()) / (series.max() - series.min())


def map_val_to_color(
    df: pd.DataFrame, column_name: str, colormap_name: str, vmin: float = None, vmax: float = None
) -> pd.DataFrame:

    df[column_name + "_scaled"] = df[column_name]

    if vmin:
        df[column_name + "_scaled"][df[column_name] < vmin] = vmin
    if vmax:
        df[column_name + "_scaled"][df[column_name] > vmax] = vmax

    df[column_name + "_scaled"] = max_min_scaler(df[column_name + "_scaled"])

    cmap = plt.get_cmap(colormap_name)
    df["fill_color"] = df.apply(lambda row: list(cmap(row[column_name + "_scaled"], bytes=True))[0:3], axis=1)
    df["fill_color"] = df.apply(lambda row: [int(i) for i in row["fill_color"]], axis=1)

    return df
