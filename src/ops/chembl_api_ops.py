from src.scripts.chembl.chembl_api_scripts import (
    get_chembl_mechanisms,
    get_chembl_assays,
    get_chembl_activities,
    get_chembl_targets,
    get_chembl_molecules,
    transform_chembl_activities,
    transform_chembl_targets,
    transform_chembl_molecules,
    transform_chembl_mechanisms,
    format_chembl_targets,
    format_chembl_mechanisms,
    format_chembl_activities,
    format_chembl_molecules,
)
from src.utils.io import save_asset
from pandas import DataFrame
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, multi_asset, op


"""Extract"""


@op
def get_chembl_mechanisms_op(context) -> DataFrame:
    df = get_chembl_mechanisms(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    return df


@op
def get_chembl_assays_op(context) -> DataFrame:
    df = get_chembl_assays(
        assay_confidence_min=context.op_config["assay_confidence_min"],
        n_jobs=context.op_config["n_jobs"],
        n_test=context.op_config["n_test"],
    )
    return df


@op
def get_chembl_activities_op(context) -> DataFrame:
    df = get_chembl_activities(
        pchembl_min=context.op_config["pchembl_min"],
        n_jobs=context.op_config["n_jobs"],
        n_test=context.op_config["n_test"],
    )
    return df


@op
def get_chembl_targets_op(context) -> DataFrame:
    df = get_chembl_targets(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    return df


@op
def get_chembl_molecules_op(context) -> DataFrame:
    df = get_chembl_molecules(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    return df


""" Transform """


@op
def transform_chembl_activities_op(activities: DataFrame, assays: DataFrame) -> DataFrame:
    return transform_chembl_activities(activities=activities, assays=assays)


@op
def transform_chembl_targets_op(targets: DataFrame) -> DataFrame:
    return transform_chembl_targets(targets=targets)


@op
def transform_chembl_molecules_op(molecules: DataFrame) -> DataFrame:
    return transform_chembl_molecules(molecules=molecules)


@op
def transform_chembl_mechanisms_op(mechanisms: DataFrame) -> DataFrame:
    return transform_chembl_mechanisms(mechanisms=mechanisms)


""" Format"""


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_target_edges": AssetOut(),
        "chembl_target_nodes": AssetOut(),
    },
)
def format_chembl_targets_asset(targets: DataFrame) -> DataFrame:
    edges, nodes = format_chembl_targets(targets=targets)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_target_edges", "chembl_target_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl targets"},
    )
    save_asset(df=edges, name="chembl_target_edges", folder="edges")
    save_asset(df=nodes, name="chembl_target_nodes", folder="nodes")

    yield Output(value=edges, output_name="chembl_target_edges")
    yield Output(value=nodes, output_name="chembl_target_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_mechanisms_edges": AssetOut(),
        "chembl_mechanisms_nodes": AssetOut(),
    },
)
def format_chembl_mechanisms_asset(mechanisms: DataFrame) -> DataFrame:
    edges, nodes = format_chembl_mechanisms(mechanisms=mechanisms)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_mechanisms_edges", "chembl_mechanisms_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl mechanisms"},
    )
    save_asset(df=edges, name="chembl_mechanism_edges", folder="edges")
    save_asset(df=nodes, name="chembl_mechanism_nodes", folder="nodes")

    yield Output(value=edges, output_name="chembl_mechanisms_edges")
    yield Output(value=nodes, output_name="chembl_mechanisms_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_activities_edges": AssetOut(),
        "chembl_activities_nodes": AssetOut(),
    },
)
def format_chembl_activities_asset(activities: DataFrame) -> DataFrame:
    edges, nodes = format_chembl_activities(activities=activities)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_activities_edges", "chembl_activities_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl activities"},
    )

    save_asset(df=edges, name="chembl_activity_edges", folder="edges")
    save_asset(df=nodes, name="chembl_activity_nodes", folder="nodes")

    yield Output(value=edges, output_name="chembl_activities_edges")
    yield Output(value=nodes, output_name="chembl_activities_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_molecules_edges": AssetOut(),
        "chembl_molecules_nodes": AssetOut(),
    },
)
def format_chembl_molecules_asset(molecules: DataFrame) -> DataFrame:
    edges, nodes = format_chembl_molecules(molecules=molecules)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_molecules_edges", "chembl_molecules_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl molecules"},
    )
    save_asset(df=edges, name="chembl_molecule_edges", folder="edges")
    save_asset(df=nodes, name="chembl_molecule_nodes", folder="nodes")

    yield Output(value=edges, output_name="chembl_molecules_edges")
    yield Output(value=nodes, output_name="chembl_molecules_nodes")
