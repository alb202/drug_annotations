from pandas import DataFrame
from pathlib import Path
from dagster import file_relative_path
from yaml import load, SafeLoader
from src.utils.postgres import Postgres

with open(file_relative_path(dunderfile=__file__, relative_path="../../config/io_config.yaml"), "r") as f:
    io_config = load(stream=f, Loader=SafeLoader)


OUTPUT_PATH = Path(io_config.get("io").get("paths").get("pickle_output")).absolute()


def create_output_folders() -> None:

    nodes = OUTPUT_PATH / "nodes"
    edges = OUTPUT_PATH / "edges"

    if not edges.exists():
        edges.mkdir(parents=True)
    if not nodes.exists():
        nodes.mkdir(parents=True)


def save_asset(df: DataFrame, name: str, folder: str) -> None:
    """Save the asset as indicated by the config file"""

    if df is None:
        return

    if io_config.get("io").get("destinations").get("pickle"):
        df_to_pickle(df=df, path=OUTPUT_PATH / folder / (name + ".pkl"))

    if io_config.get("io").get("destinations").get("postgresql"):
        df_to_postgres(df=df, table=folder, dns=io_config.get("io").get("postgresql"))


# Connectors #


def df_to_pickle(df: DataFrame, path: Path) -> None:
    """Save a DataFrame to a pickle file"""

    df.to_pickle(
        path=path,
        compression="gzip",
        protocol=4,
    )


def df_to_postgres(df: DataFrame, table: str, dns: str) -> None:
    """Append a DataFrame to a Postgres table"""
    print("dns", dns)
    db = Postgres(dns=dns)
    db.test_connection()

    db.append_df(df=df, table=table)
