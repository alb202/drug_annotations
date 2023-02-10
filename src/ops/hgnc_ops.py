from src.scripts.hgnc.hgnc_scripts import (
    get_hgnc,
    get_hgnc_additional,
    hgnc_concat_additional,
    hgnc_format,
)
from src.utils.io import save_df_asset
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, asset, multi_asset, op
from pandas import DataFrame


"""Extract"""


@asset(group_name="drh")
def get_hgnc_asset() -> DataFrame:
    return get_hgnc()


@asset(group_name="drh")
def get_hgnc_additional_asset(context, hgnc: DataFrame) -> DataFrame:
    return get_hgnc_additional(hgnc, n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])


"""Concat"""


@op
def hgnc_concat_additional_asset(hgnc_df: DataFrame, hgnc_additional: DataFrame) -> DataFrame:
    return hgnc_concat_additional(hgnc_df=hgnc_df, hgnc_additional=hgnc_additional)


"""Format"""


@multi_asset(
    group_name="hgnc",
    outs={"hgnc_edges": AssetOut(), "hgnc_nodes": AssetOut()},
)
def hgnc_format_asset(hgnc: DataFrame) -> DataFrame:
    edges, nodes = hgnc_format(hgnc=hgnc)
    yield AssetMaterialization(
        asset_key=AssetKey(("hgnc_edges", "hgnc_nodes")),
        metadata={"text_metadata": "Created edges and nodes hgnc"},
    )

    save_df_asset(df=edges, name="hgnc_edges", folder="edges")
    save_df_asset(df=nodes, name="hgnc_nodes", folder="nodes")

    yield Output(value=edges, output_name="hgnc_edges")
    yield Output(value=nodes, output_name="hgnc_nodes")
