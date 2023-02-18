from src.scripts.chembl.chembl_ftp_scripts import (
    get_chembl_structures,
    get_chembl_uniprot_mappings,
    format_chembl_structures,
    format_chembl_uniprot_mappings,
)
from src.utils.io import save_asset
from pandas import DataFrame
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, op, multi_asset


"""Extract"""


@op
def get_chembl_structures_op(context) -> DataFrame:
    df = get_chembl_structures(n_test=context.op_config.get("n_test"))
    return df


@op
def get_chembl_uniprot_mappings_op(context) -> DataFrame:
    df = get_chembl_uniprot_mappings(n_test=context.op_config.get("n_test"))
    return df


"""Format"""


@multi_asset(
    group_name="chembl_ftp_output",
    outs={"chembl_structure_edges": AssetOut()},
)
def format_chembl_structures_asset(chembl_structures: DataFrame) -> DataFrame:
    edges = format_chembl_structures(chembl_structures=chembl_structures)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_structure_edges")),
        metadata={"text_metadata": "Created edges from chembl structures"},
    )
    save_asset(df=edges, name="chembl_structure_edges", folder="edges")
    yield Output(value=edges, output_name="chembl_structure_edges")


@multi_asset(
    group_name="chembl_ftp_output",
    outs={"chembl_uniprot_mapping_edges": AssetOut()},
)
def format_chembl_uniprot_mappings_asset(chembl_uniprot_mappings: DataFrame) -> DataFrame:
    edges = format_chembl_uniprot_mappings(chembl_uniprot_mappings=chembl_uniprot_mappings)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_uniprot_mapping_edges")),
        metadata={"text_metadata": "Created edges from chembl uniprot mappings"},
    )
    save_asset(df=edges, name="chembl_uniprot_mapping_edges", folder="edges")
    yield Output(value=edges, output_name="chembl_uniprot_mapping_edges")
