from src.scripts.drug_repurposing_hub.drh_scripts import (
    get_drh_drugs,
    get_drh_samples,
    merge_drh,
    transform_drh_annotations,
    transform_drh_structures,
    transform_drh_altids,
    transform_drh_details,
    format_drh_altids,
    format_drh_annotations,
    format_drh_details,
    format_drh_structures,
)
from src.utils.io import save_asset
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, multi_asset, op
from pandas import DataFrame


"""Extract"""


@op
def get_drh_drugs_op(context) -> DataFrame:
    return get_drh_drugs(n_test=context.op_config["n_test"])


@op
def get_drh_samples_op(context) -> DataFrame:
    return get_drh_samples(n_test=context.op_config["n_test"])


"""Merge"""


@op
def merge_drh_op(samples: DataFrame, drugs: DataFrame) -> DataFrame:
    return merge_drh(samples=samples, drugs=drugs)


"""Transform"""


@op
def transform_drh_annotations_op(drugs: DataFrame) -> DataFrame:
    return transform_drh_annotations(drugs=drugs)


@op
def transform_drh_structures_op(drugs: DataFrame) -> DataFrame:
    return transform_drh_structures(drugs=drugs)


@op
def transform_drh_altids_op(drugs: DataFrame) -> DataFrame:
    return transform_drh_altids(drugs=drugs)


@op
def transform_drh_details_op(drugs: DataFrame) -> DataFrame:
    return transform_drh_details(drugs=drugs)


"""Transform"""


@multi_asset(
    group_name="drh",
    outs={"drh_annotations_edges": AssetOut()},
)
def format_drh_annotations_asset(annotations: DataFrame) -> DataFrame:
    edges = format_drh_annotations(annotations=annotations)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_annotations_edges")),
        metadata={"text_metadata": "Created edges from drh annotations"},
    )
    save_asset(df=edges, name="drh_annotations_edges", folder="edges")
    yield Output(value=edges, output_name="drh_annotations_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_structures_edges": AssetOut()},
)
def format_drh_structures_asset(structures: DataFrame) -> DataFrame:
    edges = format_drh_structures(structures=structures)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_structures_edges")),
        metadata={"text_metadata": "Created edges from drh structures"},
    )
    save_asset(df=edges, name="drh_structures_edges", folder="edges")
    yield Output(value=edges, output_name="drh_structures_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_altids_edges": AssetOut()},
)
def format_drh_altids_asset(altids: DataFrame) -> DataFrame:
    edges = format_drh_altids(altids=altids)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_altids_edges")),
        metadata={"text_metadata": "Created edges from drh altids"},
    )
    save_asset(df=edges, name="drh_altids_edges", folder="edges")
    yield Output(value=edges, output_name="drh_altids_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_annotations_nodes": AssetOut()},
)
def format_drh_details_asset(details: DataFrame) -> DataFrame:
    nodes = format_drh_details(details=details)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_annotations_nodes")),
        metadata={"text_metadata": "Created nodes from drh annotations"},
    )
    save_asset(df=nodes, name="drh_annotations_nodes", folder="nodes")
    yield Output(value=nodes, output_name="drh_annotations_nodes")
