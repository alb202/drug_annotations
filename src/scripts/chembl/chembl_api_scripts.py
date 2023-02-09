from src.scripts.chembl.chembl_api import ChemblAPI
from src.utils.formatting import reformat_edges, reformat_nodes
from pandas import DataFrame, concat
import swifter
import numpy as np


""" Extract """


def get_chembl_mechanisms(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="mechanism", max_records=n_test)
    return df


def get_chembl_assays(assay_confidence_min: int = 8, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(
        endpoint="assay",
        confidence_score_gte=assay_confidence_min,
        max_records=n_test,
    )
    return df


def get_chembl_activities(pchembl_min: int = 4, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="activity", pchembl_value_gte=pchembl_min, max_records=n_test)
    return df


def get_chembl_targets(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="target", max_records=n_test)
    return df


def get_chembl_molecules(n_test: int = 0, n_jobs: int = 2) -> DataFrame:
    api = ChemblAPI(n_jobs=n_jobs)
    df = api.retrieve_data(endpoint="molecule", max_records=n_test)
    return df


""" Transform"""


def transform_chembl_activities(activities: DataFrame, assays: DataFrame) -> DataFrame:
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
    """Targets to uniprot and gene names"""
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


def chembl_targets_format(targets: DataFrame):
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


def chembl_mechanisms_format(mechanisms: DataFrame):
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


def chembl_activities_format(activities: DataFrame):
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


def chembl_molecules_format(molecules: DataFrame):
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
