from src.scripts.chembl.chembl_ftp_scripts import (
    get_chembl_structures,
    get_chembl_uniprot_mappings,
    chembl_structures_format,
    chembl_uniprot_mappings_format,
)
from src.utils.io import save_df_asset
from pandas import DataFrame
from dagster import Output, AssetKey, AssetMaterialization, AssetOut, asset, multi_asset


"""Extract"""


@asset(group_name="chembl_ftp")
def chembl_structures_asset() -> DataFrame:
    df = get_chembl_structures()
    return df


@asset(group_name="chembl_ftp")
def chembl_uniprot_mappings_asset() -> DataFrame:
    df = get_chembl_uniprot_mappings()
    return df


"""Format"""


@multi_asset(
    group_name="chembl_ftp_output",
    outs={"chembl_structure_edges": AssetOut()},
)
def chembl_structure_format_asset(chembl_structures: DataFrame) -> DataFrame:
    edges = chembl_structures_format(chembl_structures=chembl_structures)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_structure_edges")),
        metadata={"text_metadata": "Created edges from chembl structures"},
    )
    save_df_asset(df=edges, name="chembl_structure_edges", folder="edges")
    yield Output(value=edges, output_name="chembl_structure_edges")


@multi_asset(
    group_name="chembl_ftp_output",
    outs={"chembl_uniprot_mapping_edges": AssetOut()},
)
def chembl_uniprot_mappings_format_asset(chembl_uniprot_mappings: DataFrame) -> DataFrame:
    edges = chembl_uniprot_mappings_format(chembl_uniprot_mappings=chembl_uniprot_mappings)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_uniprot_mapping_edges")),
        metadata={"text_metadata": "Created edges from chembl uniprot mappings"},
    )
    save_df_asset(df=edges, name="chembl_uniprot_mapping_edges", folder="edges")
    yield Output(value=edges, output_name="chembl_uniprot_mapping_edges")
