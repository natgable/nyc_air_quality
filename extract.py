import json
from typing import List

import geopandas as gpd
import pandas as pd
import numpy as np

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


class ACSDataHandler:
    def __init__(self, acs_vars: List[str], years: List[int]):
        """
        Instatiate ACSDataHandler to load and clean data from the American Community Survey.

        Args:
            acs_vars (List[str]): the ACS-specific variable codes
            years (List[int]): years to include in our query
        """
        self.acs_vars = acs_vars
        self.acs_vars_names = [const.ACS_VARS[var_code] for var_code in self.acs_vars]
        self.years = years
        self.zcta_df = self._load_nyc_zcta()
        self.zcta_list = ",".join([str(x) for x in list(self.zcta_df["zcta"].unique())])

    @staticmethod
    def _load_nyc_zcta() -> pd.DataFrame:
        zcta_df = pd.read_csv(const.ZCTA_TO_UHF_URL)
        zcta_df = zcta_df[zcta_df["uhfcode"] < 1000]
        return zcta_df

    def load_acs5_data(self) -> pd.DataFrame:
        """
        Using the Census Bureau API, query and load data for the given ACS Vars and years,
        at the ZCTA (zip code tabulation area) level.

        Returns:
            pd.DataFrame: Dataframe with variable, year, and ZCTA columns
        """
        all_acs_dfs = []
        for year in self.years:
            single_query = const.BASE_ZCTA_API_URL.format(
                year, ",".join(self.acs_vars), self.zcta_list
            )
            single_df = (
                pd.read_csv(single_query)
                .rename(
                    columns=dict(
                        **{"zip code tabulation area]": "zcta"}, **const.ACS_VARS
                    )
                )[list(const.ACS_VARS.values()) + ["zcta"]]
                .assign(
                    zcta=lambda df: df["zcta"].apply(lambda x: int(x.replace("]", ""))),
                    year=year,
                )
            )
            all_acs_dfs.append(single_df)
        return pd.concat(all_acs_dfs)

    def _get_most_recent_zcta_weights(self) -> pd.DataFrame:
        zcta_pop_query = const.BASE_ZCTA_API_URL.format(
            max(self.years), const.TOT_POPULATION_VAR, self.zcta_list
        )
        pop_df = (
            pd.read_csv(zcta_pop_query)
            .rename(
                columns={
                    "zip code tabulation area]": "zcta",
                    const.TOT_POPULATION_VAR: "population",
                }
            )[["zcta", "population"]]
            .assign(
                zcta=lambda df: df["zcta"].apply(lambda x: int(x.replace("]", ""))),
            )
        )
        return pop_df

    def aggregate_zcta_to_uhf(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add UHF column to dataframe and aggregate using weighted average
        (most recent ZCTA populations as weights) the desired columns.

        Args:
            df (pd.DataFrame): dataframe from ACS data

        Returns:
            pd.DataFrame: aggregated from ZCTA to UHF code level
        """
        # need to get zcta weights
        pop_weights_df = self._get_most_recent_zcta_weights()
        merge_df = pd.merge(df, self.zcta_df, on="zcta").merge(
            pop_weights_df, on="zcta"
        )
        merge_df["uhf_tot_pop"] = merge_df.groupby("uhfcode")["population"].transform(
            np.sum
        )
        merge_df["weight"] = merge_df["population"].divide(merge_df["uhf_tot_pop"])
        agg_df = (
            merge_df.groupby(["uhfcode", "year"])[list(self.acs_vars_names)]
            .sum()
            .reset_index()
        )
        return agg_df
