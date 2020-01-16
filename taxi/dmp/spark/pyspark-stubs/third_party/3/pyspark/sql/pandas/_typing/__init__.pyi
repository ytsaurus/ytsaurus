import pandas.core.frame  # type: ignore[import]
import pandas.core.series  # type: ignore[import]

from .protocols.frame import DataFrameLike
from .protocols.series import SeriesLike

from typing import Type

PandasDataFrame: Type[DataFrameLike] = pandas.core.frame.DataFrame
PandasSeries: Type[SeriesLike] = pandas.core.series.Series
