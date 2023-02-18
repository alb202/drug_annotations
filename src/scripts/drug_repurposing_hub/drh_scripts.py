from src.utils.formatting import reformat_edges, reformat_nodes
from src.utils.utilities import agg_remove_nan
from pandas import DataFrame, concat, read_csv
import swifter
import numpy as np
from pandas import isna

""" Extract """


def get_drh_drugs(n_test: int = -1) -> DataFrame:
    """Get the drug repurposing hub drug datasets

    Returns:
        DataFrame: Drug repurposing hub drug dataset

    """
    n_test = n_test if n_test > 0 else None

    url = "https://s3.amazonaws.com/data.clue.io/repurposing/downloads/repurposing_drugs_20200324.txt"

    df = read_csv(url, sep="\t", header=0, index_col=False, comment="!", nrows=n_test)
    df["moa"] = df["moa"].replace(
        "nuclear factor erythroid derived | like (NRF2) activator",
        "nuclear factor erythroid derived-like (NRF2) activator",
    )
    return df


def get_drh_samples(n_test: int = -1) -> DataFrame:
    """Get the drug repurposing hub sample datasets

    Returns:
        DataFrame: Drug repurposing hub sample dataset

    """
    n_test = n_test if n_test > 0 else None

    url = "https://s3.amazonaws.com/data.clue.io/repurposing/downloads/repurposing_samples_20200324.txt"

    df = read_csv(url, sep="\t", header=0, index_col=False, comment="!", nrows=n_test)
    df["broad_compound_id"] = df["broad_id"].apply(lambda broad_id: broad_id[:13])
    return df


""" Merge """


def merge_drh(samples: DataFrame, drugs: DataFrame) -> DataFrame:
    """Merge the drug repurposing hub drug and sample datasets"""
    return samples.merge(drugs, how="inner", on="pert_iname")


"""Transform"""


def transform_drh_annotations(drugs: DataFrame) -> DataFrame:
    """Extract the drug repurposing hub functional annotations"""

    df = drugs.loc[:, ["broad_compound_id", "moa", "target"]].drop_duplicates(subset=["broad_compound_id"])
    df["moa"] = df["moa"].swifter.apply(
        lambda moa: [(i, value) for i, value in enumerate(moa.split("|"))] if isinstance(moa, str) else moa
    )
    df["target"] = df["target"].apply(lambda target: target.split("|") if isinstance(target, str) else target)
    df = df.explode("moa").explode("target").drop_duplicates().sort_values("broad_compound_id").reset_index(drop=True)
    df["primary_moa"] = df["moa"].swifter.apply(lambda moa: moa[0] == 0 if isinstance(moa, tuple) else None)
    df["moa"] = df["moa"].swifter.apply(lambda moa: moa[1] if isinstance(moa, tuple) else None)
    return df


def transform_drh_structures(drugs: DataFrame) -> DataFrame:
    """Extract the drug repurposing hub structures"""

    df = (
        drugs.loc[:, ["broad_compound_id", "smiles", "InChIKey"]]
        .drop_duplicates()
        .sort_values("broad_compound_id")
        .reset_index(drop=True)
    )
    df["raw_smiles"] = df["smiles"].swifter.apply(lambda smiles: smiles.split(" ")[0])
    df["raw_smiles"] = np.where(df["raw_smiles"] == df["smiles"], df["raw_smiles"], None)
    df["smiles"] = df["smiles"].swifter.apply(lambda smiles: smiles.split(" ")[0])
    return df


def transform_drh_altids(drugs: DataFrame) -> DataFrame:
    """Extract the drug repurposing hub broad alternative identifiers"""

    df = drugs.loc[:, ["broad_compound_id", "deprecated_broad_id", "pubchem_cid"]].dropna(
        subset=["deprecated_broad_id", "pubchem_cid"], how="all", axis=0
    )
    df["deprecated_broad_id"] = df["deprecated_broad_id"].swifter.apply(
        lambda s: s.split("|") if isinstance(s, str) else None
    )
    df["pubchem_cid"] = df["pubchem_cid"].apply(lambda s: str(int(s)) if not isna(s) else s)
    df = df.explode("deprecated_broad_id").drop_duplicates().reset_index(drop=True)
    return df


def transform_drh_details(drugs: DataFrame) -> DataFrame:
    """Extract the drug repurposing hub additional details"""

    df = (
        drugs.loc[
            :,
            [
                "broad_compound_id",
                "pert_iname",
                "vendor",
                "vendor_name",
                "clinical_phase",
                "disease_area",
                "indication",
            ],
        ]
        .sort_values("broad_compound_id")
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df["disease_area"] = df["disease_area"].apply(lambda s: s.split("|") if isinstance(s, str) else None)
    df["indication"] = df["indication"].apply(lambda s: s.split("|") if isinstance(s, str) else None)
    df = (
        df.explode("disease_area").explode("indication").reset_index(drop=True)
    )  # .rename(columns={"pref_name": "preferred_name"})
    df = df.groupby(["broad_compound_id", "clinical_phase"]).agg(agg_remove_nan).reset_index(drop=False)
    return df


""" Formatting nodes and edges"""


def format_drh_annotations(annotations: DataFrame) -> DataFrame:
    """Format the drug repurposing hub annotations.

    Args:
        annotations (DataFrame): The drug repurposing hub annotations.

    Returns:
        DataFrame: DRH annotation edges dataframe.
    """
    edge_1 = reformat_edges(
        df=annotations,
        from_type_val="broad_compound_identifier",
        from_value="broad_compound_id",
        to_type_val="mechanism_of_action",
        to_value="moa",
        label_val="has_mechanism_of_action",
        source_val="drug_repurposing_hub",
        parameters=["primary_moa"],
    )

    edge_2 = reformat_edges(
        df=annotations,
        from_type_val="broad_compound_identifier",
        from_value="broad_compound_id",
        to_type_val="gene_symbol",
        to_value="target",
        label_val="has_protein_target",
        source_val="drug_repurposing_hub",
        parameters=None,
    )
    return concat([edge_1, edge_2]).reset_index(drop=True)


def format_drh_structures(structures: DataFrame) -> DataFrame:
    """Format the drug repurposing hub structures.

    Args:
        structures (DataFrame): The drug repurposing hub structures.

    Returns:
        DataFrame: DRH structure edges dataframe.
    """
    edge_1 = reformat_edges(
        df=structures,
        from_type_val="smiles",
        from_value="smiles",
        to_type_val="broad_compound_identifier",
        to_value="broad_compound_id",
        label_val="has_structure",
        source_val="drug_repurposing_hub",
        parameters=["raw_smiles"],
    )

    edge_2 = reformat_edges(
        df=structures,
        from_type_val="inchi_key",
        from_value="InChIKey",
        to_type_val="broad_compound_identifier",
        to_value="broad_compound_id",
        label_val="has_inchi_key",
        source_val="drug_repurposing_hub",
        parameters=None,
    )
    return concat([edge_1, edge_2]).reset_index(drop=True)


def format_drh_altids(altids: DataFrame) -> DataFrame:
    """Format the drug repurposing hub alternative identifiers

    Args:
        altids (DataFrame): The drug repurposing hub alternative identifiers.

    Returns:
        DataFrame: DRH alternative identifiers edges dataframe.
    """
    edge_1 = reformat_edges(
        df=altids,
        from_type_val="broad_sample_identifier",
        from_value="deprecated_broad_id",
        to_type_val="broad_compound_identifier",
        to_value="broad_compound_id",
        label_val="has_alternative_id",
        source_val="drug_repurposing_hub",
        parameters=None,
    )

    edge_2 = reformat_edges(
        df=altids,
        from_type_val="broad_compound_identifier",
        from_value="broad_compound_id",
        to_type_val="pubchem_id",
        to_value="pubchem_cid",
        label_val="has_equivilant_id",
        source_val="drug_repurposing_hub",
        parameters=None,
    )
    return concat([edge_1, edge_2]).reset_index(drop=True)


def format_drh_details(details: DataFrame) -> DataFrame:
    """Format the drug repurposing hub compound details

    Args:
        details (DataFrame): The drug repurposing hub compound details.

    Returns:
        DataFrame: DRH compound details edges dataframe.
    """
    node_1 = reformat_nodes(
        df=details,
        node_type_val="broad_compound_identifier",
        value="broad_compound_id",
        source_val="drug_repurposing_hub",
        parameters=["clinical_phase", "pert_iname", "vendor", "vendor_name", "disease_area", "indication"],
    )
    return concat([node_1]).reset_index(drop=True)
