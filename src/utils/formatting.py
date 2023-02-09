from yaml import load, SafeLoader
from pathlib import Path
from pandas import DataFrame

config_dict = load(open(Path("src/config/formatting_config.yaml").absolute(), "r"), Loader=SafeLoader)


def rename_column(df: DataFrame, new_column: str, old_column: str = None, column_value: bool = True) -> DataFrame:
    """Renames columns during during formatting of edge and node dataframes"""
    if not isinstance(new_column, str):
        raise ValueError("A column name must be provided as a string")
    elif not isinstance(old_column, str) and not isinstance(column_value, str):
        raise ValueError("An existing column must be a string, or a new column value must be provided")
    elif isinstance(old_column, str) and isinstance(column_value, str):
        raise ValueError("Provide either an existing column name or a new column value")
    elif isinstance(old_column, str) and old_column not in df.columns:
        raise ValueError(f"The existing column '{old_column}' was not found in the dataframe")
    elif new_column == old_column:
        pass
    elif isinstance(old_column, str) and old_column in df.columns:
        df[new_column] = df[old_column]
    else:
        df[new_column] = column_value
    return df


def reformat_edges(
    df: DataFrame,
    from_type: str = None,
    from_value: str = None,
    to_type: str = None,
    to_value: str = None,
    label: str = None,
    source: str = None,
    parameters: list = None,
    from_type_val: str = None,
    from_value_val: str = None,
    to_type_val: str = None,
    to_value_val: str = None,
    label_val: str = None,
    source_val: str = None,
) -> DataFrame:
    """Reformat edge dataframes and parameters"""
    parameters = [] if not parameters else parameters
    df = rename_column(df=df, new_column="from_type", old_column=from_type, column_value=from_type_val)
    df = rename_column(df=df, new_column="from_value", old_column=from_value, column_value=from_value_val)
    df = rename_column(df=df, new_column="label", old_column=label, column_value=label_val)
    df = rename_column(df=df, new_column="to_type", old_column=to_type, column_value=to_type_val)
    df = rename_column(df=df, new_column="to_value", old_column=to_value, column_value=to_value_val)
    df = rename_column(df=df, new_column="source", old_column=source, column_value=source_val)

    invalid_from_types = [
        from_type
        for from_type in df["from_type"].unique()
        if from_type not in config_dict.get("formatting").get("node_types")
    ]
    if invalid_from_types:
        raise (f'From types {".".join(invalid_from_types)} are not valid')

    invalid_to_types = [
        to_type for to_type in df["to_type"].unique() if to_type not in config_dict.get("formatting").get("node_types")
    ]
    if invalid_to_types:
        raise (f'To types {".".join(invalid_to_types)} are not valid')

    df["parameters"] = df.loc[:, parameters].to_dict(orient="records") if len(parameters) else None

    df = (
        df.loc[:, ["from_type", "from_value", "label", "to_type", "to_value", "source", "parameters"]]
        # .dropna(how="any", subset=["from_type", "from_value", "label", "to_type", "to_value", "source"], axis=0)
        .drop_duplicates(["from_type", "from_value", "label", "to_type", "to_value", "source"], keep="first")
        .sort_values(["from_type", "from_value", "label", "to_type", "to_value", "source"])
        .reset_index(drop=True)
    )
    return df


def reformat_nodes(
    df: DataFrame,
    node_type: str = None,
    value: str = None,
    source: str = None,
    parameters: list = None,
    node_type_val: str = None,
    source_val: str = None,
    value_val: str = None,
) -> DataFrame:

    """Reformat dataframe to a list of nodes and parameters"""
    parameters = [] if not parameters else parameters

    df = rename_column(df=df, new_column="node_type", old_column=node_type, column_value=node_type_val)
    df = rename_column(df=df, new_column="value", old_column=value, column_value=value_val)
    df = rename_column(df=df, new_column="source", old_column=source, column_value=source_val)

    invalid_node_types = [
        node_type
        for node_type in df["node_type"].unique()
        if node_type not in config_dict.get("formatting").get("node_types")
    ]
    if invalid_node_types:
        raise (f'Node types {".".join (invalid_node_types)} are not valid')
    df["parameters"] = df.loc[:, parameters].to_dict(orient="records") if len(parameters) else None
    df = (
        df.loc[:, ["node_type", "value", "source", "parameters"]]
        # .dropna(how="any", subset=["node_type", "value", "source"], axis=0)
        .drop_duplicates(["node_type", "value", "source"], keep="first")
        .sort_values(["node_type", "value", "source"])
        .reset_index(drop=True)
    )
    return df
