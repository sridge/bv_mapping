import pydeck as pdk
import pandas as pd
import matplotlib.pyplot as plt


def max_min_scaler(series: pd.Series) -> pd.Series:
    return (series - series.min()) / (series.max() - series.min())


def map_val_to_color(
    df: pd.DataFrame, column_name: str, colormap_name: str, vmin: float = None, vmax: float = None
) -> pd.DataFrame:

    if vmin:
        df[column_name][df[column_name] < vmin] = vmin
    if vmax:
        df[column_name][df[column_name] > vmax] = vmax

    df[column_name + "_scaled"] = max_min_scaler(df[column_name])

    cmap = plt.get_cmap(colormap_name)
    df["fill_color"] = df.apply(lambda row: list(cmap(row[column_name + "_scaled"], bytes=True))[0:3], axis=1)
    df["fill_color"] = df.apply(lambda row: [int(i) for i in row["fill_color"]], axis=1)

    return df


malady = "overhang"

HEX1 = f"{malady}_areas.json"
HEX2 = f"all_{malady}_areas.json"

df_1 = pd.read_json(HEX1)
df_1 = df_1.set_index("hex")
df_2 = pd.read_json(HEX2)
df_2 = df_2.set_index("hex")

df_2["pct_pos"] = df_1["count"] / df_2["count"]
df_2["pct_pos"][~df_2.index.isin(df_1.index)] = 0
df = df_2.reset_index()
df = df[df["count"] > 10].round(decimals=2)

df = map_val_to_color(df, "pct_pos", "viridis")

# Define a layer to display on a map
layer = pdk.Layer(
    "H3HexagonLayer",
    data=df.to_dict(orient="records"),
    pickable=True,
    stroked=True,
    filled=True,
    extruded=False,
    get_hexagon="hex",
    opacity=0.8,
    get_fill_color="fill_color",
    get_line_color=[255, 255, 255],
    line_width_min_pixels=0.12,
)

# Set the viewport location

# 39.8283° N, 98.5795° W
view_state = pdk.ViewState(latitude=39.8283, longitude=-98.5795, zoom=3, bearing=0)


# Render
r = pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "Fraction Positive: {pct_pos}"})
r.to_html(f"{malady}.html")
