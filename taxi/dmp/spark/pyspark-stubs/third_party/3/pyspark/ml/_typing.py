from typing import Any, Dict, TypeVar, Union
from typing_extensions import Literal

import pyspark.ml.base
import pyspark.ml.param
import pyspark.ml.util
import pyspark.ml.wrapper

ParamMap = Dict[pyspark.ml.param.Param, Any]
PipelineStage = Union[pyspark.ml.base.Estimator, pyspark.ml.base.Transformer]

T = TypeVar("T")
P = TypeVar("P", bound=pyspark.ml.param.Params)
M = TypeVar("M", bound=pyspark.ml.base.Transformer)
JM = TypeVar("JM", bound=pyspark.ml.wrapper.JavaTransformer)

BinaryClassificationEvaluatorMetricType = Union[
    Literal["areaUnderROC"], Literal["areaUnderPR"]
]
RegressionEvaluatorMetricType = Union[
    Literal["rmse"], Literal["mse"], Literal["r2"], Literal["mae"], Literal["var"]
]
MulticlassClassificationEvaluatorMetricType = Union[
    Literal["f1"],
    Literal["accuracy"],
    Literal["weightedPrecision"],
    Literal["weightedRecall"],
    Literal["weightedTruePositiveRate"],
    Literal["weightedFalsePositiveRate"],
    Literal["weightedFMeasure"],
    Literal["truePositiveRateByLabel"],
    Literal["falsePositiveRateByLabel"],
    Literal["precisionByLabel"],
    Literal["recallByLabel"],
    Literal["fMeasureByLabel"],
]
MultilabelClassificationEvaluatorMetricType = Union[
    Literal["subsetAccuracy"],
    Literal["accuracy"],
    Literal["hammingLoss"],
    Literal["precision"],
    Literal["recall"],
    Literal["f1Measure"],
    Literal["precisionByLabel"],
    Literal["recallByLabel"],
    Literal["f1MeasureByLabel"],
    Literal["microPrecision"],
    Literal["microRecall"],
    Literal["microF1Measure"],
]
ClusteringEvaluatorMetricType = Union[Literal["silhouette"]]
RankingEvaluatorMetricType = Union[
    Literal["meanAveragePrecision"],
    Literal["meanAveragePrecisionAtK"],
    Literal["precisionAtK"],
    Literal["ndcgAtK"],
    Literal["recallAtK"],
]
