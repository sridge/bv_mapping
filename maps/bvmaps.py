import pydeck as pdk
import pandas as pd
import map_helpers as mhelpers
from bp.database import db_session
from bp.constants import ONTOLOGY_MAPPING


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


class PreProcessScatter(object):
    def __init__(
        self,
        carrier,
        colormap: str = "RdYlGn",
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

            df = pd.read_csv(f"scores_{self.carrier}.csv")
            df = mhelpers.map_val_to_color(df, "score_binned", self.colormap, self.vmin, self.vmax)

        self.data = df

        if "fill_color" not in self.data.columns:
            raise ValueError("fill_color not set for data")

    def render_map(self, map_name: str = None):

        if not map_name:
            map_name = f"{self.carrier}.html"

        # Define a layer to display on a map
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=self.data,
            pickable=True,
            stroked=True,
            filled=True,
            opacity=0.8,
            get_position=["unit_longitude", "unit_latitude"],
            get_fill_color="fill_color",
            get_radius=1000,
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
            tooltip={"text": "Roof score: {score_binned}"},
        )
        r.to_html(map_name)

        return r

    def generate_map(self):

        self.process_data()
        self.render_map()


class MaladyHexmap(object):
    def __init__(self, vcid, vmin=None, vmax=None):

        self.vcid = vcid
        self.vmin = vmin
        self.vmax = vmax

    def process_data(self, df: pd.Series = None):

        if not df:

            with db_session() as session:
                # get classifier malady name
                query = f"""
                SELECT
                    SUBSTRING( name 7 )
                FROM
                    auto_ingest_predict
                    JOIN annotation_type ON annotation_type_id = annotation_type.id
                WHERE
                    vision_classifier_id = {self.vcid}
                """
                df_gt_0 = pd.read_sql(query, session.get_bind())
                self.malady = df.loc[0].values

                # get predictions with area > 0
                query = f"""
                SELECT
                    (data_points -> 'dturk_{ONTOLOGY_MAPPING[self.malady]}')::FLOAT AS area_malady,
                    latitude,
                    longitude,
                    property_id
                FROM
                    annotation
                    JOIN gtu ON gtu.id = gtu_id
                WHERE
                    (data_points -> 'dturk_{ONTOLOGY_MAPPING[self.malady]}')::FLOAT > 0
                    AND vision_classifier_id = {self.vcid}
                """
                df_gt_0 = pd.read_sql(query, session.get_bind())
                df_gt_0 = mhelpers.add_h3(df_gt_0)
                df_gt_0 = mhelpers.count_h3(df_gt_0)

                df_gt_0_fname = f"all_{self.malady}_areas.json"
                df_gt_0 = mhelpers.df_to_deck_json(df_gt_0, df_gt_0_fname)

                # get all predictions
                f"""
                SELECT
                    (data_points -> 'dturk_{ONTOLOGY_MAPPING[self.malady]}')::FLOAT AS area_malady,
                    latitude,
                    longitude,
                    property_id
                FROM
                    annotation
                    JOIN gtu ON gtu.id = gtu_id
                WHERE
                    vision_classifier_id = {self.vcid}
                """
                df_all = pd.read_sql(query, session.get_bind())
                df_all = mhelpers.add_h3(df_all)
                df_all = mhelpers.count_h3(df_all)

                df_all_fname = f"all_{self.malady}_areas.json"
                df_all = mhelpers.df_to_deck_json(df_all, df_all_fname)

            df_gt_0 = pd.read_json(df_gt_0_fname)
            df_gt_0 = df_gt_0.set_index(df_gt_0_fname)
            df_all = pd.read_json(df_all_fname)
            df_all = df_all.set_index("hex")

            df_all["pct_pos"] = df_gt_0["count"] / df_all["count"]
            df_all["pct_pos"][~df_all.index.isin(df_gt_0.index)] = 0
            df = df_all.reset_index()
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
    MaladyHexmap("overhang").generate_map()
    # PreProcessHexmap("Tuscacora").generate_map()
