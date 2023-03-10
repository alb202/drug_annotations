from yaml import load, SafeLoader
from pandas import DataFrame
from dagster import file_relative_path

with open(file_relative_path(dunderfile=__file__, relative_path="../../config/formatting_config.yaml"), "r") as f:
    config_dict = load(stream=f, Loader=SafeLoader)


class InvalidColumnError(Exception):
    """Raised when an new column is not provided"""

    def __init__(self, msg=str):
        super().__init__(msg)


class InvalidTypeError(Exception):
    """Raised when an invalid type is provided"""

    def __init__(self, types: list):
        self.types = ",".join(types)
        self.message = f"Type(s) '{self.types}' are not valid"
        super().__init__(self.message)


class InvalidSourceError(Exception):
    """Raised when an invalid source is provided"""

    def __init__(self, sources: list):
        self.sources = ",".join(sources)
        self.message = f"Source(s) '{self.sources}' are not valid"
        super().__init__(self.message)


def rename_column(df: DataFrame, new_column: str, old_column: str = None, column_value: bool = True) -> DataFrame:
    """Renames columns during during formatting of edge and node dataframes"""

    if not new_column:
        raise InvalidColumnError("A new column must be provided")
    elif not old_column and not column_value:
        raise InvalidColumnError("An existing column or a new column value must be provided")
    elif old_column and column_value:
        raise InvalidColumnError("Provide either an existing column name or a new column value")
    elif old_column and old_column not in df.columns:
        raise InvalidColumnError(f"The existing column '{old_column}' was not found in the dataframe")
    elif new_column == old_column:
        pass
    elif old_column and old_column in df.columns:
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
        if from_type not in config_dict.get("validation").get("node_types")
    ]
    if invalid_from_types:
        raise InvalidTypeError(types=invalid_from_types)

    invalid_to_types = [
        to_type for to_type in df["to_type"].unique() if to_type not in config_dict.get("validation").get("node_types")
    ]
    if invalid_to_types:
        raise InvalidTypeError(types=invalid_to_types)

    invalid_sources = [
        source for source in df["source"].unique() if source not in config_dict.get("validation").get("sources")
    ]
    if invalid_sources:
        raise InvalidTypeError(types=invalid_sources)

    df["parameters"] = df.loc[:, parameters].to_dict(orient="records") if len(parameters) else None

    return (
        df.loc[:, ["from_type", "from_value", "label", "to_type", "to_value", "source", "parameters"]]
        .dropna(how="any", subset=["from_type", "from_value", "label", "to_type", "to_value", "source"], axis=0)
        .drop_duplicates(["from_type", "from_value", "label", "to_type", "to_value", "source"], keep="first")
        .sort_values(["from_type", "from_value", "label", "to_type", "to_value", "source"])
        .reset_index(drop=True)
    )


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
        if node_type not in config_dict.get("validation").get("node_types")
    ]
    if invalid_node_types:
        raise InvalidTypeError(types=invalid_node_types)

    invalid_sources = [
        source for source in df["source"].unique() if source not in config_dict.get("validation").get("sources")
    ]
    if invalid_sources:
        raise InvalidSourceError(types=invalid_sources)

    df["parameters"] = df.loc[:, parameters].to_dict(orient="records") if len(parameters) else None

    return (
        df.loc[:, ["node_type", "value", "source", "parameters"]]
        .dropna(how="any", subset=["node_type", "value", "source"], axis=0)
        .drop_duplicates(["node_type", "value", "source"], keep="first")
        .sort_values(["node_type", "value", "source"])
        .reset_index(drop=True)
    )
