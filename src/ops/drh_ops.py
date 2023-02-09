from src.scripts.drug_repurposing_hub.drh_scripts import (
    get_drh_drugs,
    get_drh_samples,
    merge_drh,
    transform_drh_annotations,
    transform_drh_structures,
    transform_drh_altids,
    transform_drh_details,
    drh_annotations_format,
    drh_structures_format,
    drh_altids_format,
    drh_details_format,
)
from src.utils.io import save_df_asset
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, asset, multi_asset, op
from pandas import DataFrame


"""Extract"""


@asset(group_name="drh")
def drh_drugs_asset() -> DataFrame:
    return get_drh_drugs()


@asset(group_name="drh")
def drh_samples_asset() -> DataFrame:
    return get_drh_samples()


"""Merge"""


@op
def drh_merged(samples, drugs) -> DataFrame:
    return merge_drh(samples=samples, drugs=drugs)


"""Transform"""


@op
def drh_annotations_op(drugs) -> DataFrame:
    return transform_drh_annotations(drugs=drugs)


@op
def drh_structures_op(drugs) -> DataFrame:
    return transform_drh_structures(drugs=drugs)


@op
def drh_altids_op(drugs) -> DataFrame:
    return transform_drh_altids(drugs=drugs)


@op
def drh_details_op(drugs) -> DataFrame:
    return transform_drh_details(drugs=drugs)


"""Transform"""


@multi_asset(
    group_name="drh",
    outs={"drh_annotations_edges": AssetOut()},
)
def drh_annotations_format_asset(annotations) -> DataFrame:
    edges = drh_annotations_format(annotations=annotations)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_annotations_edges")),
        metadata={"text_metadata": "Created edges from drh annotations"},
    )
    save_df_asset(df=edges, name="drh_annotations_edges")
    yield Output(value=edges, output_name="drh_annotations_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_structures_edges": AssetOut()},
)
def drh_structures_format_asset(structures) -> DataFrame:
    edges = drh_structures_format(structures=structures)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_structures_edges")),
        metadata={"text_metadata": "Created edges from drh structures"},
    )
    save_df_asset(df=edges, name="drh_structures_edges")
    yield Output(value=edges, output_name="drh_structures_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_altids_edges": AssetOut()},
)
def drh_altids_format_asset(altids) -> DataFrame:
    edges = drh_altids_format(altids=altids)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_altids_edges")),
        metadata={"text_metadata": "Created edges from drh altids"},
    )
    save_df_asset(df=edges, name="drh_altids_edges")
    yield Output(value=edges, output_name="drh_altids_edges")


@multi_asset(
    group_name="drh",
    outs={"drh_annotations_nodes": AssetOut()},
)
def drh_details_format_asset(details) -> DataFrame:
    nodes = drh_details_format(details=details)
    yield AssetMaterialization(
        asset_key=AssetKey(("drh_annotations_nodes")),
        metadata={"text_metadata": "Created nodes from drh annotations"},
    )
    save_df_asset(df=nodes, name="drh_annotations_nodes")
    yield Output(value=nodes, output_name="drh_annotations_nodes")
