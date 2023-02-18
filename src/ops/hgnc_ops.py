from src.scripts.hgnc.hgnc_scripts import (
    get_hgnc,
    get_hgnc_additional,
    concat_hgnc_additional,
    format_hgnc,
)
from src.utils.io import save_asset
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, multi_asset, op
from pandas import DataFrame


"""Extract"""


@op
def get_hgnc_op(context) -> DataFrame:
    return get_hgnc(n_test=context.op_config["n_test"])


@op
def get_hgnc_additional_op(context, hgnc: DataFrame) -> DataFrame:
    return get_hgnc_additional(hgnc, n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])


"""Concat"""


@op
def concat_hgnc_additional_op(hgnc_df: DataFrame, hgnc_additional: DataFrame) -> DataFrame:
    return concat_hgnc_additional(hgnc_df=hgnc_df, hgnc_additional=hgnc_additional)


"""Format"""


@multi_asset(
    group_name="hgnc",
    outs={"hgnc_edges": AssetOut(), "hgnc_nodes": AssetOut()},
)
def format_hgnc_asset(hgnc: DataFrame) -> DataFrame:
    edges, nodes = format_hgnc(hgnc=hgnc)
    yield AssetMaterialization(
        asset_key=AssetKey(("hgnc_edges", "hgnc_nodes")),
        metadata={"text_metadata": "Created edges and nodes hgnc"},
    )

    save_asset(df=edges, name="hgnc_edges", folder="edges")
    save_asset(df=nodes, name="hgnc_nodes", folder="nodes")

    yield Output(value=edges, output_name="hgnc_edges")
    yield Output(value=nodes, output_name="hgnc_nodes")
