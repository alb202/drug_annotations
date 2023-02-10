from src.utils.formatting import reformat_edges
from pandas import DataFrame, read_csv, concat
import numpy as np
import swifter


def get_chembl_structures() -> DataFrame:
    """Get the molecular structures from the chembl FTP.


    Returns:
        DataFrame: A dataframe containing the molecular structures.

    """

    url = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_31/chembl_31_chemreps.txt.gz"

    df = (
        read_csv(url, compression="gzip", comment=None, sep="\t", header=0)
        .loc[:, ["chembl_id", "canonical_smiles", "standard_inchi"]]
        .rename(columns={"chembl_id": "chembl_compound_id", "canonical_smiles": "smiles", "standard_inchi": "inchi"})
    )
    return df


def get_chembl_uniprot_mappings() -> DataFrame:
    """Get the uniprot mappings from the chembl FTP.

    Returns:
        DataFrame: A dataframe containing the uniprot mappings.

    """
    url = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases/chembl_31/chembl_uniprot_mapping.txt"

    df = read_csv(
        url,
        comment="#",
        sep="\t",
        header=None,
        names=["uniprot_accession", "chembl_target_id", "name", "target_type"],
    )
    df["target_type"] = np.where(df["target_type"] == "SINGLE PROTEIN", "is_protein", "includes_protein")
    df["uniprot_accession"] = df["uniprot_accession"].swifter.apply(lambda s: s.strip())
    return df


def chembl_structures_format(chembl_structures: DataFrame) -> DataFrame:
    """Format the molecular structures datasets.

    Args:
        chembl_structures (DataFrame): The molecular structure dataframe

    Returns:
        DataFrame: Molecular structure edges dataframe.

    """
    edge_1 = reformat_edges(
        df=chembl_structures,
        from_type_val="smiles",
        from_value="smiles",
        to_type_val="chembl_compound_id",
        to_value="chembl_compound_id",
        label_val="has_structure",
        source_val="chembl",
        parameters=None,
    )

    edge_2 = reformat_edges(
        df=chembl_structures,
        from_type_val="inchi",
        from_value="inchi",
        to_type_val="chembl_compound_id",
        to_value="chembl_compound_id",
        label_val="has_structure",
        source_val="chembl",
        parameters=None,
    )
    return concat([edge_1, edge_2]).reset_index(drop=True)


def chembl_uniprot_mappings_format(chembl_uniprot_mappings: DataFrame) -> DataFrame:
    """Format the uniprot mappings datasets.

    Args:
        chembl_uniprot_mappings (DataFrame): The uniprot mapping dataframe

    Returns:
        DataFrame: Molecular structure edges dataframe.

    """
    edge_1 = reformat_edges(
        df=chembl_uniprot_mappings,
        from_type_val="chembl_target_id",
        from_value="chembl_target_id",
        to_type_val="uniprot_accession",
        to_value="uniprot_accession",
        label_val="target_type",
        source_val="chembl",
        parameters=None,
    )

    return concat([edge_1]).reset_index(drop=True)
