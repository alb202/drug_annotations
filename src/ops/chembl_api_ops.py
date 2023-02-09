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
    chembl_targets_format,
    chembl_mechanisms_format,
    chembl_activities_format,
    chembl_molecules_format,
)
from src.utils.io import save_df_asset
from pandas import DataFrame
from dagster import (
    get_dagster_logger,
    asset,
    op,
    AssetKey,
    AssetMaterialization,
    multi_asset,
    AssetOut,
    Output
)


# class MyIOManager(IOManager):
#     def handle_output(self, context, obj):
#         write_csv("some/path")

# @io_manager
# def my_io_manager(init_context):
#     return MyIOManager()


@asset(group_name="chembl_api")
def chembl_mechanisms_asset(context) -> DataFrame:
    df = get_chembl_mechanisms(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    get_dagster_logger().info(f"Length of chembl mechanisms: {len(df)}")
    return df


@asset(group_name="chembl_api")
def chembl_assays_asset(context) -> DataFrame:
    df = get_chembl_assays(
        assay_confidence_min=context.op_config["assay_confidence_min"],
        n_jobs=context.op_config["n_jobs"],
        n_test=context.op_config["n_test"],
    )
    get_dagster_logger().info(f"Length of chembl assays: {len(df)}")
    return df


@asset(group_name="chembl_api")
def chembl_activities_asset(context) -> DataFrame:
    df = get_chembl_activities(
        pchembl_min=context.op_config["pchembl_min"],
        n_jobs=context.op_config["n_jobs"],
        n_test=context.op_config["n_test"],
    )
    get_dagster_logger().info(f"Length of chembl activities: {len(df)}")
    return df


@asset(group_name="chembl_api")
def chembl_targets_asset(context) -> DataFrame:
    df = get_chembl_targets(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    get_dagster_logger().info(f"Length of chembl targets: {len(df)}")
    return df


@asset(group_name="chembl_api")
def chembl_molecules_asset(context) -> DataFrame:
    df = get_chembl_molecules(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
    get_dagster_logger().info(f"Length of chembl molecules: {len(df)}")
    return df


# transform_chembl_activities,
# transform_chembl_targets,
# transform_chembl_molecules,
# transform_chembl_mechanisms,

""" Transform """


@op
def chembl_activities_transform_asset(activities, assays) -> DataFrame:
    return transform_chembl_activities(activities=activities, assays=assays)


@op
def chembl_targets_transform_asset(targets) -> DataFrame:
    return transform_chembl_targets(targets=targets)


@op
def chembl_molecules_transform_asset(molecules) -> DataFrame:
    return transform_chembl_molecules(molecules=molecules)


@op
def chembl_mechanisms_transform_asset(mechanisms) -> DataFrame:
    return transform_chembl_mechanisms(mechanisms=mechanisms)


""" Format"""


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_target_edges": AssetOut(),
        "chembl_target_nodes": AssetOut(),
    },
)
def chembl_targets_format_asset(targets) -> DataFrame:
    edges, nodes = chembl_targets_format(targets=targets)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_target_edges", "chembl_target_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl targets"},
    )
    save_df_asset(df=edges, name="checmbl_target_edges")
    save_df_asset(df=nodes, name="checmbl_target_nodes")

    yield Output(value=edges, output_name="chembl_target_edges")
    yield Output(value=nodes, output_name="chembl_target_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_mechanisms_edges": AssetOut(),
        "chembl_mechanisms_nodes": AssetOut(),
    },
)
def chembl_mechanisms_format_asset(mechanisms) -> DataFrame:
    edges, nodes = chembl_mechanisms_format(mechanisms=mechanisms)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_mechanisms_edges", "chembl_mechanisms_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl mechanisms"},
    )
    save_df_asset(df=edges, name="checmbl_mechanism_edges")
    save_df_asset(df=nodes, name="checmbl_mechanism_nodes")

    yield Output(value=edges, output_name="chembl_mechanisms_edges")
    yield Output(value=nodes, output_name="chembl_mechanisms_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_activities_edges": AssetOut(),
        "chembl_activities_nodes": AssetOut(),
    },
)
def chembl_activities_format_asset(activities) -> DataFrame:
    edges, nodes = chembl_activities_format(activities=activities)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_activities_edges", "chembl_activities_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl activities"},
    )

    save_df_asset(df=edges, name="checmbl_activity_edges")
    save_df_asset(df=nodes, name="checmbl_activity_nodes")

    yield Output(value=edges, output_name="chembl_activities_edges")
    yield Output(value=nodes, output_name="chembl_activities_nodes")


@multi_asset(
    group_name="chembl_api_output",
    outs={
        "chembl_molecules_edges": AssetOut(),
        "chembl_molecules_nodes": AssetOut(),
    },
)
def chembl_molecules_format_asset(molecules) -> DataFrame:
    edges, nodes = chembl_molecules_format(molecules=molecules)
    yield AssetMaterialization(
        asset_key=AssetKey(("chembl_molecules_edges", "chembl_molecules_nodes")),
        metadata={"text_metadata": "Created edges and nodes from chembl molecules"},
    )
    save_df_asset(df=edges, name="checmbl_molecule_edges")
    save_df_asset(df=nodes, name="checmbl_molecule_nodes")

    yield Output(value=edges, output_name="chembl_molecules_edges")
    yield Output(value=nodes, output_name="chembl_molecules_nodes")
    #     pchembl_min=context.op_config["pchembl_min"],
    #     n_jobs=context.op_config["n_jobs"],
    #     n_test=context.op_config["n_test"],
    # )
    # get_dagster_logger().info(f"Length of chembl activities: {len(df)}")
    # return df


# @asset
# def chembl_assays_transform_asset(context) -> DataFrame:
#     df = get_chembl_assays(
#         assay_confidence_min=context.op_config["assay_confidence_min"],
#         n_jobs=context.op_config["n_jobs"],
#         n_test=context.op_config["n_test"],
#     )
#     get_dagster_logger().info(f"Length of chembl assays: {len(df)}")
#     return df


# @asset
# def chembl_activities_transform_asset(df) -> DataFrame:
#     df = transform_chembl_activities(df)
#     #     pchembl_min=context.op_config["pchembl_min"],
#     #     n_jobs=context.op_config["n_jobs"],
#     #     n_test=context.op_config["n_test"],
#     # )
#     # get_dagster_logger().info(f"Length of chembl activities: {len(df)}")
#     return df


# @asset
# def chembl_targets_transform_asset(context) -> DataFrame:
#     df = get_chembl_targets(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
#     get_dagster_logger().info(f"Length of chembl targets: {len(df)}")
#     return df


# @asset
# def chembl_molecules_transform_asset(context) -> DataFrame:
#     df = get_chembl_molecules(n_jobs=context.op_config["n_jobs"], n_test=context.op_config["n_test"])
#     get_dagster_logger().info(f"Length of chembl molecules: {len(df)}")
#     return df


# @asset(
#     # ins={"n_test": In(dagster_type=Int), "n_jobs": In(dagster_type=Int)},
#     # config_schema={"n_test": int, "n_jobs": int},
# )
# def get_chembl_mechanisms(context) -> DataFrame:
#     api = ChemblAPI(n_jobs=context.op_config["n_jobs"])
#     df = api.retrieve_data(endpoint="mechanism", max_records=context.op_config["n_test"])
#     get_dagster_logger().info(f"Length of chembl mechanisms: {len(df)}")
#     return df


# @asset(
#     # ins={"assay_confidence_min": In(dagster_type=Int), "n_test": In(dagster_type=Int), "n_jobs": In(dagster_type=Int)},
#     # config_schema={"n_test": int, "n_jobs": int, "assay_confidence_min": int},
# )
# def get_chembl_assays(context, assay_confidence_min: int = 8, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
#     api = ChemblAPI(n_jobs=context.op_config["n_jobs"])
#     df = api.retrieve_data(
#         endpoint="assay",
#         confidence_score_gte=context.op_config["assay_confidence_min"],
#         max_records=context.op_config["n_test"],
#     )
#     get_dagster_logger().info(f"Length of chembl assays: {len(df)}")
#     return df


# @asset(
#     ins={"pchembl_min": In(dagster_type=Int), "n_test": In(dagster_type=Int), "n_jobs": In(dagster_type=Int)},
#     config_schema={"n_test": int, "n_jobs": int, "pchembl_min": int},
# )
# def get_chembl_activities(context, pchembl_min: int = 4, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
#     api = ChemblAPI(n_jobs=context.op_config["n_jobs"])
#     df = api.retrieve_data(
#         endpoint="activity", pchembl_value_gte=context.op_config["pchembl_min"], max_records=context.op_config["n_test"]
#     )
#     get_dagster_logger().info(f"Length of chembl activities: {len(df)}")
#     return df


# @asset(
#     ins={"n_test": In(dagster_type=Int), "n_jobs": In(dagster_type=Int)},
#     config_schema={"n_test": int, "n_jobs": int},
# )
# def get_chembl_targets(context, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
#     api = ChemblAPI(n_jobs=context.op_config["n_jobs"])
#     df = api.retrieve_data(endpoint="target", max_records=context.op_config["n_test"])
#     get_dagster_logger().info(f"Length of chembl targets: {len(df)}")
#     return df


# @asset(
#     ins={"n_test": In(dagster_type=Int), "n_jobs": In(dagster_type=Int)},
#     config_schema={"n_test": int, "n_jobs": int},
# )
# def get_chembl_molecules(context, n_test: int = 0, n_jobs: int = 2) -> DataFrame:
#     api = ChemblAPI(n_jobs=context.op_config["n_jobs"])
#     df = api.retrieve_data(endpoint="molecule", max_records=context.op_config["n_test"])
#     get_dagster_logger().info(f"Length of chembl molecules: {len(df)}")
#     return df


# from pathlib import Path

# from urllib3.util.retry import Retry
# from urllib3.util.timeout import Timeout
# from tqdm import tqdm

# tqdm.pandas()
# os.chdir(path='/Users/blumea03/OneDrive - Pfizer/Dagster/Projects/ChemKG/src')


# os.chdir(path='/Users/blumea03/OneDrive - Pfizer/Dagster/Common/Python')

# OUTPUT_PATH = Path("/Users/ab/OneDrive/projects/drugkg/data/raw")
# TEMP_PATH = Path("/Users/ab/OneDrive/projects/drugkg/data/temp")
# FILES_PATH = Path("/Users/blumea03/OneDrive - Pfizer/Dagster/Projects/ChemKG/files/")

# api = ChemblAPI(n_jobs=2)

# n_test = 500
# pCHEMBL_MIN = 4
# ASSAY_CONFIDENCE_MIN = 8


# # assays = api.retrieve_data(endpoint="assay", confidence_score_gte=ASSAY_CONFIDENCE_MIN, max_records=n_test)
# # assays.to_pickle(TEMP_PATH / "chembl_assays.pkl", compression="gzip", protocol=4)

# activities = api.retrieve_data(endpoint="activity", pchembl_value_gte=pCHEMBL_MIN, max_records=n_test)
# activities.to_pickle(TEMP_PATH / "chembl_activities.pkl", compression="gzip", protocol=4)

# targets = api.retrieve_data(endpoint="target", max_records=n_test)
# targets.to_pickle(TEMP_PATH / "chembl_targets.pkl", compression="gzip", protocol=4)

# molecules = api.retrieve_data(endpoint="molecule", max_records=n_test)
# molecules.to_pickle(TEMP_PATH / "chembl_molecules.pkl", compression="gzip", protocol=4)

# """ Load data """
# mechanisms = pd.read_pickle(TEMP_PATH / "chembl_mechanisms.pkl", compression="gzip")
# targets = pd.read_pickle(TEMP_PATH / "chembl_targets.pkl", compression="gzip")
# molecules = pd.read_pickle(TEMP_PATH / "chembl_molecules.pkl", compression="gzip")
# assays = pd.read_pickle(TEMP_PATH / "chembl_assays.pkl", compression="gzip")
# activities = pd.read_pickle(TEMP_PATH / "chembl_activities.pkl", compression="gzip")

# """ Process data """
# assays = assays.loc[
#     :, ["assay_chembl_id", "confidence_description", "confidence_score", "description", "document_chembl_id"]
# ].drop_duplicates()
# activities = activities.merge(
#     assays.loc[:, ["assay_chembl_id", "confidence_description", "confidence_score", "description"]],
#     how="inner",
#     on="assay_chembl_id",
# )
# molecules["parent_chembl_id"] = molecules["molecule_hierarchy"].progress_apply(
#     lambda x: x.get("molecule_chembl_id") if isinstance(x, dict) else None
# )
# molecules["parent_chembl_id"] = np.where(
#     molecules["parent_chembl_id"].isnull(), molecules["molecule_chembl_id"], molecules["parent_chembl_id"]
# )
# molecule_parameters = [
#     "chirality",
#     "max_phase",
#     "molecule_type",
#     "pref_name",
#     "prodrug",
#     "structure_type",
#     "withdrawn_class",
#     "withdrawn_country",
#     "withdrawn_flag",
#     "withdrawn_reason",
#     "withdrawn_year",
# ]
# molecules = molecules.loc[:, ["molecule_chembl_id", "parent_chembl_id"] + molecule_parameters].rename(
#     columns={"pref_name": "preferred_name"}
# )


# """Targets to uniprot and gene names"""
# # targets targets.copy(deep=True)
# targets = targets.explode("target_components").reset_index(drop=True)
# targets["uniprot_accession"] = targets["target_components"].progress_apply(
#     lambda s: s.get("accession") if isinstance(s, dict) else None
# )
# targets["uniprot_accession"] = targets["uniprot_accession"].progress_apply(
#     lambda s: s.strip() if isinstance(s, str) else None
# )

# # targets['species'] = targets['tax_id'].progress_apply(lambda t: int(t) if not pd.isna(t) else None)
# targets["uniprot_relationship"] = targets["target_components"].progress_apply(
#     lambda s: s.get("relationship").strip() if isinstance(s, dict) else None
# )
# targets["uniprot_relationship"] = targets["uniprot_relationship"].progress_apply(
#     lambda s: s.strip() if isinstance(s, str) else None
# )

# targets["gene_symbol"] = targets["target_components"].progress_apply(
#     lambda s: [
#         i.get("component_synonym").strip()
#         for i in s.get("target_component_synonyms")
#         if i.get("syn_type") == "GENE_SYMBOL"
#     ]
#     if isinstance(s, dict)
#     else None
# )
# targets = (
#     targets.explode("gene_symbol")
#     .sort_values("target_chembl_id")
#     .reset_index(drop=True)
#     .drop(["cross_references", "target_components"], axis=1)
# )
# targets["uniprot_relationship"] = targets["uniprot_relationship"].progress_apply(
#     lambda s: "is_" + s.lower().replace(" ", "") if isinstance(s, str) else None
# )
# uniprot__id_check = [
#     validate_uniprot_accession(uniprot_accession) for uniprot_accession in tqdm(targets["uniprot_accession"])
# ]
# chembl_id_check = [validate_chembl_id(chembl_id) for chembl_id in tqdm(targets["target_chembl_id"])]


# """ Lists for edge and node dataframes"""
# edge_list = []
# node_list = []


# """Mechanisms """

# """ Map chembl compound to chembl target """
# chembl_compound_mechanism_target = reformat_edge_df(
#     df=mechanisms.copy(deep=True),
#     from_type="chembl_compound_id",
#     from_value="molecule_chembl_id",
#     to_type="chembl_target_id",
#     to_value="target_chembl_id",
#     label="has_mechanism_target",
#     source="chembl",
#     param_cols=["mechanism_comment", "action_type", "direct_interaction"],
#     is_from_type=False,
#     is_from_value=True,
#     is_to_type=False,
#     is_to_value=True,
#     is_label=False,
#     is_source=False,
# )
# edge_list.append(chembl_compound_mechanism_target)
# chembl_compound_mechanism_target


# """Map chembl compound to mechanism of action"""
# chembl_compound_mechanism = reformat_edge_df(
#     df=mechanisms.copy(deep=True),
#     from_type="chembl_compound_id",
#     from_value="parent_molecule_chembl_id",
#     to_type="mechanism_of_action",
#     to_value="mechanism_of_action",
#     label="has_moa",
#     source="chembl",
#     param_cols=["mechanism_comment", "action_type", "direct_interaction"],
#     is_from_type=False,
#     is_from_value=True,
#     is_to_type=False,
#     is_to_value=True,
#     is_label=False,
#     is_source=False,
# )
# edge_list.append(chembl_compound_mechanism)
# chembl_compound_mechanism


# """ Activity """


# """ Map chembl compound to parent """
# chembl_activity_targets = reformat_edge_df(
#     df=activities.copy(deep=True),
#     from_type="chembl_compound_id",
#     from_value="molecule_chembl_id",
#     to_type="chembl_target_id",
#     to_value="target_chembl_id",
#     label="has_activity_target",
#     source="chembl",
#     param_cols=["confidence_description", "confidence_score", "description", "pchembl_value"],
#     is_from_type=False,
#     is_from_value=True,
#     is_to_type=False,
#     is_to_value=True,
#     is_label=False,
#     is_source=False,
# )
# edge_list.append(chembl_activity_targets)
# chembl_activity_targets

# """ Targets"""


# """ Map chembl target to uniprot """
# chembl_target_proteins = reformat_edge_df(
#     df=targets.query("uniprot_accession == uniprot_accession").copy(deep=True),
#     from_type="chembl_target_id",
#     from_value="target_chembl_id",
#     to_type="uniprot_accession",
#     to_value="uniprot_accession",
#     label="has_protein",
#     source="chembl",
#     param_cols=["uniprot_relationship"],
#     is_from_type=False,
#     is_from_value=True,
#     is_to_type=False,
#     is_to_value=True,
#     is_label=False,
#     is_source=False,
# )
# edge_list.append(chembl_target_proteins)
# chembl_target_proteins


# """ Map chembl target to uniprot """
# chembl_target_gene_symbols = reformat_edge_df(
#     df=targets.query("gene_symbol == gene_symbol").copy(deep=True),
#     from_type="chembl_target_id",
#     from_value="target_chembl_id",
#     to_type="gene_symbol",
#     to_value="gene_symbol",
#     label="has_gene_symbol",
#     source="chembl",
#     param_cols=["uniprot_relationship"],
#     is_from_type=False,
#     is_from_value=True,
#     is_to_type=False,
#     is_to_value=True,
#     is_label=False,
#     is_source=False,
# )
# edge_list.append(chembl_target_gene_symbols)
# chembl_target_gene_symbols


# """ Add parameters to chembl target nodes """
# chembl_targets_parameters = reformat_node_df(
#     df=targets.copy(deep=True).rename(columns={"pref_name": "preferred_name"}),
#     node_type="chembl_target_id",
#     value="target_chembl_id",
#     source="chembl",
#     param_cols=["organism", "target_type", "preferred_name"],
#     is_node_type=False,
#     is_value=True,
#     is_source=False,
# )


# """ Molecules """


# """ Add parameters to chembl nodes """
# chembl_molecule_parameters = reformat_node_df(
#     df=molecules.copy(deep=True),
#     node_type="chembl_compound_id",
#     value="molecule_chembl_id",
#     source="chembl",
#     param_cols=[
#         "preferred_name",
#         "chirality",
#         "max_phase",
#         "molecule_type",
#         "prodrug",
#         "structure_type",
#         "withdrawn_class",
#         "withdrawn_country",
#         "withdrawn_flag",
#         "withdrawn_reason",
#         "withdrawn_year",
#     ],
#     is_node_type=False,
#     is_value=True,
#     is_source=False,
# )
# node_list.append(chembl_molecule_parameters)
# chembl_molecule_parameters

# pd.concat(edge_list).reset_index(drop=True).to_parquet(OUTPUT_PATH / "edges" / "chembl_activity_e.parquet")
# pd.concat(node_list).reset_index(drop=True).to_parquet(OUTPUT_PATH / "nodes" / "chembl_activity_v.parquet")
