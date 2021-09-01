import pydeck as pdk
import pandas as pd
import map_helpers as mhelpers


class PreProcessHexmap(object):
    def __init__(
        self,
        carrier,
        colormap: str = "RdYlGn_r",
        vmin: float = None,
        vmax: float = None,
        latitude: float = 37.0,
        longitude: float = -96.0,
        zoom: int = 4,
    ):

        self.carrier = carrier
        self.vmin = vmin
        self.vmax = vmax
        self.latitude = latitude
        self.longitude = longitude
        self.zoom = zoom
        self.colormap = colormap

    def process_data(self, df: pd.Series = None):

        if not df:

            df = mhelpers.add_h3(f"scores_{self.carrier}.csv", lon_col="unit_longitude", lat_col="unit_latitude")
            df, df_2 = mhelpers.count_h3(df, column_mapper={"unit_id": "count"})
            df, df_1 = mhelpers.count_h3(df[df["score_binned"] <= 1], column_mapper={"unit_id": "count"})

            df_2["pct"] = df_1["count"] / df_2["count"]
            df_2["pct"][~df_2.index.isin(df_1.index)] = 0
            df = df_2.reset_index()
            df = df[df["count"] > 10].round(decimals=2)

            df = mhelpers.map_val_to_color(df, "pct", self.colormap, self.vmin, self.vmax)

        self.data = df

        if "fill_color" not in self.data.columns:
            raise ValueError("fill_color not set for data")

    def render_map(self, map_name: str = None):

        if not map_name:
            map_name = f"{self.carrier}.html"

        # Define a layer to display on a map
        layer = pdk.Layer(
            "H3HexagonLayer",
            data=self.data,
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
        # Center of US 39.8283° N, 98.5795° W
        view_state = pdk.ViewState(latitude=self.latitude, longitude=self.longitude, zoom=self.zoom, bearing=0)

        # Render
        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Fraction Low Roof Score: {pct}\nTotal Number of Properties {count}"},
        )
        r.to_html(map_name)

        return r

    def generate_map(self):

        self.process_data()
        self.render_map()


class MaladyHexmap(object):
    def __init__(self, malady, vmin=None, vmax=None):

        self.malady = malady
        self.vmin = vmin
        self.vmax = vmax

    def process_data(self, df: pd.Series = None):

        if not df:

            HEX1 = f"{self.malady}_areas.json"
            HEX2 = f"all_{self.malady}_areas.json"

            df_1 = pd.read_json(HEX1)
            df_1 = df_1.set_index("hex")
            df_2 = pd.read_json(HEX2)
            df_2 = df_2.set_index("hex")

            df_2["pct_pos"] = df_1["count"] / df_2["count"]
            df_2["pct_pos"][~df_2.index.isin(df_1.index)] = 0
            df = df_2.reset_index()
            df = df[df["count"] > 10].round(decimals=2)

            df = mhelpers.map_val_to_color(df, "pct_pos", "viridis", vmin=self.vmin, vmax=self.vmax)

        self.data = df

        if "fill_color" not in self.data.columns:
            raise ValueError("fill_color not set for data")

    def render_map(self, map_name: str = None):

        if not map_name:
            map_name = f"{self.malady}.html"

        # Define a layer to display on a map
        layer = pdk.Layer(
            "H3HexagonLayer",
            data=self.data,
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
        # Center of US 39.8283° N, 98.5795° W
        view_state = pdk.ViewState(latitude=37, longitude=-96, zoom=4, bearing=0)

        # Render
        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip={"text": "Fraction Positive: {pct_pos}\nCount {count}"},
        )
        r.to_html(map_name)

        return r

    def generate_map(self):
        self.process_data()
        self.render_map()


if __name__ == "__main__":

    PreProcessHexmap("Tuscacora").generate_map()
