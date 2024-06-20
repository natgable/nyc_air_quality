import json

import geopandas as gpd
import pandas as pd

import constants as const


def handle_nyc_open_data(api_endpoint_json: str) -> pd.DataFrame:
    """
    Get data from the NYC Open Data API endpoint. Since the row limit is 1000 for API
    reads, loop through until all data is read and return as a pandas DataFrame.

    Args:
        api_endpoint_json (str): API endpoint. Must end in .json

    Returns:
        pd.DataFrame: the data as a pandas DataFrame
    """
    all_dfs, idx = [], 0
    while True:
        partial_df = pd.read_json(
            f"{api_endpoint_json}?$limit={const.API_ROW_LIMIT}&$offset={idx}"
        )
        if not partial_df.empty:
            all_dfs.append(partial_df)
        else:
            break
        idx += const.API_ROW_LIMIT
    return pd.concat(all_dfs)


def load_and_merge_geo_data() -> gpd.GeoDataFrame:
    """
    Provided geospatial data can be found at https://github.com/nychealth/EHDP-data/tree/production/geography.
    This function loads the provided geometries for UHF 34, UHF 42, and CD geography types. This does not do any
    spatial aggregation or comparison between these geographies, but rather returns then as a single GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: with columns, geography, geo_type_name, and geo_join_id
    """
    geometries_uhf_42 = gpd.read_file(const.UHF_42_SHP_URL)
    geometries_uhf_34 = gpd.read_file(const.UHF_34_SHP_URL)
    geometries_cd = gpd.read_file(const.CD_SHP_URL)
    # need a single dataframe with geo_type_name, geo_join_id, and geometry so we can get a geometry column assigned to the air quality dataset
    full_gdf = gpd.GeoDataFrame(
        pd.concat(
            [
                geometries_uhf_34[["UHF34_CODE", "geometry"]]
                .assign(geo_type_name="UHF34")
                .rename(columns={"UHF34_CODE": "geo_join_id"}),
                geometries_uhf_42[["UHFCODE", "geometry"]]
                .assign(geo_type_name="UHF42")
                .rename(columns={"UHFCODE": "geo_join_id"}),
                geometries_cd[["BoroCD", "geometry"]]
                .assign(geo_type_name="CD")
                .rename(columns={"BoroCD": "geo_join_id"}),
            ]
        )
    )
    return full_gdf


def handle_acs_data():
    pass
