from src.scripts.hgnc.hgnc_api import HGNC
from src.utils.formatting import reformat_edges, reformat_nodes
from typing import Tuple
from pandas import DataFrame, read_csv, isna, concat
import swifter
import numpy as np


def get_hgnc(n_test: int = -1) -> DataFrame:
    """Get the full table of HGNC genes.

    Returns:
        DataFrame: Primary table of HGNC genes.
    """
    n_test = n_test if n_test > 0 else None

    query_columns = [
        "gd_hgnc_id",
        "gd_app_sym",
        "gd_app_name",
        "gd_status",
        "gd_aliases",
        "gd_pub_chrom_map",
        "gd_locus_type",
        "gd_locus_group",
        "gd_pub_eg_id",
        "family.name",
        "gd_prev_sym",
        "md_prot_id",
    ]
    rename_columns = {
        "HGNC ID": "hgnc_gene_id",
        "Approved symbol": "approved_gene_symbol",
        "Approved name": "gene_name",
        "Status": "status",
        "Alias symbols": "alias_gene_name",
        "Chromosome": "chromosome",
        "Locus type": "gene_locus_type",
        "Locus group": "gene_locus_group",
        "NCBI Gene ID": "ncbi_gene_id",
        "Gene group name": "gene_group_name",
        "Previous symbols": "previous_gene_name",
        "UniProt ID(supplied by UniProt)": "uniprot_id",
    }
    url_columns = "&".join(["col=" + column for column in query_columns])
    hgnc_url = f"https://www.genenames.org/cgi-bin/download/custom?status=Approved&hgnc_dbtag=on&order_by=gd_hgnc_id&format=text&submit=submit&{url_columns}"

    df = read_csv(hgnc_url, sep="\t", header=0, nrows=n_test).rename(columns=rename_columns)

    df["ncbi_gene_id"] = df["ncbi_gene_id"].swifter.apply(lambda x: str(int(x)) if not isna(x) else None)
    df["gene_group_name"] = (
        df["gene_group_name"]
        .swifter.apply(lambda x: [i.strip() for i in x.split(",") if i != ""] if not isna(x) else None)
        .replace([], None)
    )
    df["alias_gene_name"] = (
        df["alias_gene_name"]
        .swifter.apply(lambda x: [i.upper().strip() for i in x.split(",")] if not isna(x) else None)
        .replace([], None)
    )
    df["previous_gene_name"] = (
        df["previous_gene_name"]
        .swifter.apply(lambda x: [i.upper().strip() for i in x.split(",")] if not isna(x) else None)
        .replace([], None)
    )
    df["uniprot_id"] = df["uniprot_id"].swifter.apply(
        lambda x: [i.strip() for i in x.split(",")] if not isna(x) else None
    )
    return df


def get_hgnc_additional(hgnc: DataFrame, n_jobs: int = 4, n_test: int = -1) -> DataFrame:
    """Get additional information from HGNC for genes missing NCBI ID annotations in the main table

    Args:
        hgnc (DataFrame): The full table of HGNC genes.
        n_jobs (int, optional): Number of jobs for HGNC API to use. Defaults to 4.
        n_test (int, optional): Number of additional genes to query during testing. Defaults to -1.

    Returns:
        DataFrame: Additional information from HGNC for genes missing NCBI ID annotations.
    """
    rename_columns = {
        "hgnc_id": "hgnc_gene_id",
        "symbol": "approved_gene_symbol",
        "name": "gene_name",
        "status": "status",
        "alias_symbol": "alias_gene_name",
        "location": "chromosome",
        "locus_type": "gene_locus_type",
        "locus_group": "gene_locus_group",
        "entrez_id": "ncbi_gene_id",
        "gene_group": "gene_group_name",
        "prev_symbol": "previous_gene_name",
        "uniprot_ids": "uniprot_id",
    }

    get_hgnc_details = HGNC(n_jobs=2)
    id_list = (
        hgnc.query("ncbi_gene_id != ncbi_gene_id")["hgnc_gene_id"]
        .dropna()
        .drop_duplicates()
        .swifter.apply(lambda s: s.replace("HGNC:", ""))
        .to_list()
    )
    id_list = id_list if n_test < 1 else id_list[:n_test]
    hgnc_details = get_hgnc_details.fetch_ids(id_list=id_list)
    df = DataFrame(hgnc_details).loc[:, rename_columns.keys()].rename(columns=rename_columns)
    return df


def concat_hgnc_additional(hgnc_df: DataFrame, hgnc_additional: DataFrame) -> DataFrame:
    """Concatenate additional information from HGNC for genes missing NCBI ID annotations"""
    df = concat(
        [
            hgnc_df.loc[
                (hgnc_df["ncbi_gene_id"] == hgnc_df["ncbi_gene_id"])
                | ~hgnc_df["hgnc_gene_id"].isin(hgnc_additional["hgnc_gene_id"].drop_duplicates())
            ],
            hgnc_additional.replace(np.NaN, None),
        ],
        axis=0,
    )
    df["species"] = "Homo sapiens"
    df = (
        df.explode("alias_gene_name")
        .explode("gene_group_name")
        .explode("uniprot_id")
        .explode("previous_gene_name")
        .reset_index(drop=True)
    )
    return df


def format_hgnc(hgnc: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Format the hgnc gene datasets.

    Args:
        hgnc (DataFrame): The hgnc gene dataframe

    Returns:
        (DataFrame, DataFrame): hgnc edges and nodes dataframes.

    """
    edge_1 = reformat_edges(
        df=hgnc,
        from_type_val="gene_symbol",
        from_value="approved_gene_symbol",
        to_type_val="hgnc",
        to_value="hgnc_gene_id",
        label_val="is_approved_symbol",
        source_val="hgnc",
        parameters=None,
    )

    edge_2 = reformat_edges(
        df=hgnc,
        from_type_val="gene_symbol",
        from_value="previous_gene_name",
        to_type_val="hgnc",
        to_value="hgnc_gene_id",
        label_val="is_previous_symbol",
        source_val="hgnc",
        parameters=None,
    )

    edge_3 = reformat_edges(
        df=hgnc,
        from_type_val="gene_symbol",
        from_value="alias_gene_name",
        to_type_val="hgnc",
        to_value="hgnc_gene_id",
        label_val="is_alias_symbol",
        source_val="hgnc",
        parameters=None,
    )

    edge_4 = reformat_edges(
        df=hgnc,
        from_type_val="hgnc",
        from_value="hgnc_gene_id",
        to_type_val="uniprot_accession",
        to_value="uniprot_id",
        label_val="is_protein",
        source_val="hgnc",
        parameters=None,
    )

    node_1 = reformat_nodes(
        df=hgnc.groupby(["hgnc_gene_id", "gene_name", "gene_locus_type", "gene_locus_group", "species"])
        .agg(list)
        .loc[:, ["gene_group_name"]]
        .reset_index(drop=False),
        node_type_val="hgnc",
        value="hgnc_gene_id",
        source_val="hgnc",
        parameters=["gene_group_name", "gene_name", "gene_locus_type", "gene_locus_group", "species"],
    )
    return concat([edge_1, edge_2, edge_3, edge_4]).reset_index(drop=True), concat([node_1]).reset_index(drop=True)
