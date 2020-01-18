# This module contains POC compatibility annotations

import pandas.core.frame  # type: ignore[import]
import pandas.core.series  # type: ignore[import]

from .protocols.frame import DataFrameLike as DataFrameLike
from .protocols.series import SeriesLike as SeriesLike

from typing import Type

PandasDataFrame: Type[DataFrameLike] = pandas.core.frame.DataFrame
PandasSeries: Type[SeriesLike] = pandas.core.series.Series
