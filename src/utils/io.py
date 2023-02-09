from pandas import DataFrame


def save_df_asset(df: DataFrame, name: str) -> None:
    """Save a DataFrame to a pickle file"""
    if isinstance(df, DataFrame):
        df.to_pickle(
            path="/mnt/" + name + ".pkl",
            compression="gzip",
            protocol=4,
        )
