from src.scripts.chembl.chembl_api import ChemblAPI
from src.utils.formatting import reformat_edges, reformat_nodes
from pandas import DataFrame, concat
import swifter
import numpy as np
from typing import Tuple


""" Extract """


def get_chembl_mechanisms(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    """Get the chemical mechanisms from Chembl.

    Args:
        n_test (int, optional): Number of records to get. Defaults to 0.
        n_jobs (int, optional): Number of concurrent jobs to use in the ChEMBL API. Defaults to 2.

    Returns:
        DataFrame: The chemical mechanisms from Chembl.
    """
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="mechanism", max_records=n_test)
    return df


def get_chembl_assays(assay_confidence_min: int = 8, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    """Get the chemical assays from Chembl.

    Args:
        assay_confidence_min (int, optional): A minimum confidence level from the assay table. Defaults to 8.
        n_test (int, optional): Number of records to get. Defaults to 0.
        n_jobs (int, optional): Number of concurrent jobs to use in the ChEMBL API. Defaults to 2.

    Returns:
        DataFrame: The chemical assays from Chembl.
    """
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(
        endpoint="assay",
        confidence_score_gte=assay_confidence_min,
        max_records=n_test,
    )
    return df


def get_chembl_activities(pchembl_min: int = 4, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    """Get the chemical activities from Chembl.

    Args:
        pchembl_min (int, optional): A minimum pChembl value from the activity table. Defaults to 4.
        n_test (int, optional): Number of records to get. Defaults to 0.
        n_jobs (int, optional): Number of concurrent jobs to use in the ChEMBL API. Defaults to 2.

    Returns:
        DataFrame: The chemical activities from Chembl.
    """
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="activity", pchembl_value_gte=pchembl_min, max_records=n_test)
    return df


def get_chembl_targets(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    """Get the chemical targets from Chembl.

    Args:
        n_test (int, optional): Number of records to get. Defaults to 0.
        n_jobs (int, optional): Number of concurrent jobs to use in the ChEMBL API. Defaults to 2.

    Returns:
        DataFrame: The chemical targets from Chembl.
    """
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="target", max_records=n_test)
    return df


def get_chembl_molecules(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    """Get the chemical molecules from Chembl.

    Args:
        n_test (int, optional): Number of records to get. Defaults to 0.
        n_jobs (int, optional): Number of concurrent jobs to use in the ChEMBL API. Defaults to 2.

    Returns:
        DataFrame: The chemical molecules from Chembl.
    """
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="molecule", max_records=n_test)
    return df


""" Transform"""


def transform_chembl_activities(activities: DataFrame, assays: DataFrame) -> DataFrame:
    """Transform the chemical activities from Chembl."""

    assays = assays.loc[
        :, ["assay_chembl_id", "confidence_description", "confidence_score", "description", "document_chembl_id"]
    ].drop_duplicates()
    df = activities.merge(
        assays.loc[:, ["assay_chembl_id", "confidence_description", "confidence_score", "description"]],
        how="inner",
        on="assay_chembl_id",
    )
    return df


def transform_chembl_targets(targets: DataFrame) -> DataFrame:
    """Transform the chemical targets from Chembl."""

    df = targets.explode("target_components").reset_index(drop=True)
    df["uniprot_accession"] = df["target_components"].swifter.apply(
        lambda s: s.get("accession") if isinstance(s, dict) else None
    )
    df["uniprot_accession"] = df["uniprot_accession"].swifter.apply(lambda s: s.strip() if isinstance(s, str) else None)

    df["uniprot_relationship"] = df["target_components"].swifter.apply(
        lambda s: s.get("relationship").strip() if isinstance(s, dict) else None
    )
    df["uniprot_relationship"] = df["uniprot_relationship"].swifter.apply(
        lambda s: s.strip() if isinstance(s, str) else None
    )

    df["gene_symbol"] = df["target_components"].swifter.apply(
        lambda s: [
            i.get("component_synonym").strip()
            for i in s.get("target_component_synonyms")
            if i.get("syn_type") == "GENE_SYMBOL"
        ]
        if isinstance(s, dict)
        else None
    )

    df = (
        df.explode("gene_symbol")
        .sort_values("target_chembl_id")
        .reset_index(drop=True)
        .drop(["cross_references", "target_components"], axis=1)
    )

    df["uniprot_relationship"] = df["uniprot_relationship"].swifter.apply(
        lambda s: "is_" + s.lower().replace(" ", "") if isinstance(s, str) else None
    )

    return df


def transform_chembl_molecules(molecules: DataFrame) -> DataFrame:
    """Transform the chemical molecules from Chembl."""

    molecules["parent_chembl_id"] = molecules["molecule_hierarchy"].swifter.apply(
        lambda x: x.get("molecule_chembl_id") if isinstance(x, dict) else None
    )
    molecules["parent_chembl_id"] = np.where(
        molecules["parent_chembl_id"].isnull(), molecules["molecule_chembl_id"], molecules["parent_chembl_id"]
    )
    molecule_parameters = [
        "chirality",
        "max_phase",
        "molecule_type",
        "pref_name",
        "prodrug",
        "structure_type",
        "withdrawn_class",
        "withdrawn_country",
        "withdrawn_flag",
        "withdrawn_reason",
        "withdrawn_year",
    ]
    df = molecules.loc[:, ["molecule_chembl_id", "parent_chembl_id"] + molecule_parameters].rename(
        columns={"pref_name": "preferred_name"}
    )
    return df


def transform_chembl_mechanisms(mechanisms: DataFrame) -> DataFrame:
    """Transform the chemical mechanisms from Chembl."""

    df = mechanisms.loc[
        :,
        [
            "molecule_chembl_id",
            "parent_molecule_chembl_id",
            "mechanism_of_action",
            "target_chembl_id",
            "mechanism_comment",
            "action_type",
            "direct_interaction",
        ],
    ]
    return df


""" Formatting nodes and edges"""


def format_chembl_targets(targets: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Format the chemical targets data into edge and node datasets."""

    edge_1 = reformat_edges(
        df=targets,
        from_value="target_chembl_id",
        to_value="uniprot_accession",
        parameters=["uniprot_relationship", "organism"],
        from_type_val="chembl_target_id",
        to_type_val="uniprot_accession",
        label_val="has_protein",
        source_val="chembl",
    )

    edge_2 = reformat_edges(
        df=targets.rename(columns={"pref_name": "preferred_name"}),
        from_value="target_chembl_id",
        to_value="gene_symbol",
        parameters=["uniprot_relationship", "organism"],
        from_type_val="chembl_target_id",
        to_type_val="gene_symbol",
        label_val="has_gene_symbol",
        source_val="chembl",
    )

    node_1 = reformat_nodes(
        df=targets.rename(columns={"pref_name": "preferred_name"}),
        node_type_val="chembl_target_id",
        value="target_chembl_id",
        source_val="chembl",
        parameters=["organism", "target_type", "preferred_name"],
    )
    return concat([edge_1, edge_2]).reset_index(drop=True), concat([node_1]).reset_index(drop=True)


def format_chembl_mechanisms(mechanisms: DataFrame):
    """Format the chemical mechanisms data into edge datasets."""

    edge_1 = reformat_edges(
        df=mechanisms,
        from_type_val="chembl_compound_id",
        from_value="molecule_chembl_id",
        to_type_val="chembl_target_id",
        to_value="target_chembl_id",
        label_val="has_mechanism_target",
        source_val="chembl",
        parameters=["mechanism_comment", "action_type", "direct_interaction"],
    )

    edge_2 = reformat_edges(
        df=mechanisms,
        from_type_val="chembl_compound_id",
        from_value="parent_molecule_chembl_id",
        to_type_val="mechanism_of_action",
        to_value="mechanism_of_action",
        label_val="has_moa",
        source_val="chembl",
        parameters=["mechanism_comment", "action_type", "direct_interaction"],
    )
    return concat([edge_1, edge_2]).reset_index(drop=True), None


def format_chembl_activities(activities: DataFrame):
    """Format the chemical activities data into edge datasets."""

    edge_1 = reformat_edges(
        df=activities,
        from_type_val="chembl_compound_id",
        from_value="molecule_chembl_id",
        to_type_val="chembl_target_id",
        to_value="target_chembl_id",
        label_val="has_activity_target",
        source_val="chembl",
        parameters=["confidence_description", "confidence_score", "description", "pchembl_value"],
    )

    return concat([edge_1]).reset_index(drop=True), None


def format_chembl_molecules(molecules: DataFrame):
    """Format the molecule data into node datasets."""

    node_1 = reformat_nodes(
        df=molecules,
        node_type_val="chembl_compound_id",
        value="molecule_chembl_id",
        source_val="chembl",
        parameters=[
            "preferred_name",
            "chirality",
            "max_phase",
            "molecule_type",
            "prodrug",
            "structure_type",
            "withdrawn_class",
            "withdrawn_country",
            "withdrawn_flag",
            "withdrawn_reason",
            "withdrawn_year",
        ],
    )
    return None, concat([node_1]).reset_index(drop=True)
