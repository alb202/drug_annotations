from pandas import DataFrame
from pathlib import Path
from dagster import file_relative_path
from yaml import load, SafeLoader


with open(file_relative_path(dunderfile=__file__, relative_path="../config/io_config.yaml"), "r") as f:
    config_dict = load(stream=f, Loader=SafeLoader)

OUTPUT_PATH = Path(config_dict.get("io").get("paths").get("output"))


def create_output_folders() -> None:

    nodes = OUTPUT_PATH / "nodes"
    edges = OUTPUT_PATH / "edges"

    if not edges.exists():
        edges.mkdir()
    if not nodes.exists():
        nodes.mkdir()


def save_df_asset(df: DataFrame, name: str, folder: str) -> None:
    """Save a DataFrame to a pickle file"""

    if isinstance(df, DataFrame):
        df.to_pickle(
            path=OUTPUT_PATH / folder / (name + ".pkl"),
            compression="gzip",
            protocol=4,
        )
