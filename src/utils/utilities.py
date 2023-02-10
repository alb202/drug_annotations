from pandas.core.groupby.generic import DataFrameGroupBy
from pandas import isna


def agg_remove_nan(lst: DataFrameGroupBy) -> list:
    """Remove NaN values from a DataFrameGroupBy object"""
    return list(filter(lambda value: not isna(value), list(set(lst))))
