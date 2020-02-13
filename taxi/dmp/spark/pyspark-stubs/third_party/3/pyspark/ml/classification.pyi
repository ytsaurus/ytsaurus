# Stubs for pyspark.ml.classification (Python 3)

import abc
from typing import Any, Dict, List, Optional, TypeVar
from pyspark.ml._typing import JM, M, P, T, ParamMap

from pyspark.ml.base import Estimator, Model, Transformer
from pyspark.ml.linalg import Matrix, Vector
from pyspark.ml.param.shared import *
from pyspark.ml.tree import (
    _DecisionTreeModel,
    _DecisionTreeParams,
    _TreeEnsembleModel,
    _RandomForestParams,
    _GBTParams,
    _HasVarianceImpurity,
    _TreeClassifierParams,
    _TreeEnsembleParams,
)
from pyspark.ml.regression import (
    _FactorizationMachinesParams,
    DecisionTreeRegressionModel,
)
from pyspark.ml.util import *
from pyspark.ml.wrapper import (
    JavaPredictionModel,
    JavaPredictor,
    _JavaPredictorParams,
    JavaWrapper,
    JavaTransformer,
)
from pyspark.sql.dataframe import DataFrame

class _JavaClassifierParams(HasRawPredictionCol, _JavaPredictorParams): ...

class JavaClassifier(JavaPredictor[JM], _JavaClassifierParams, metaclass=abc.ABCMeta):
    def setRawPredictionCol(self: P, value: str) -> P: ...

class JavaClassificationModel(JavaPredictionModel[T], _JavaClassifierParams):
    def setRawPredictionCol(self: P, value: str) -> P: ...
    @property
    def numClasses(self) -> int: ...
    def predictRaw(self, value: Vector) -> Vector: ...

class _JavaProbabilisticClassifierParams(
    HasProbabilityCol, HasThresholds, _JavaClassifierParams
): ...

class JavaProbabilisticClassifier(
    JavaClassifier[JM], _JavaProbabilisticClassifierParams, metaclass=abc.ABCMeta
):
    def setProbabilityCol(self: P, value: str) -> P: ...
    def setThresholds(self: P, value: List[float]) -> P: ...

class JavaProbabilisticClassificationModel(
    JavaClassificationModel[T], _JavaProbabilisticClassifierParams
):
    def setProbabilityCol(self: P, value: str) -> P: ...
    def setThresholds(self, value: List[float]) -> P: ...
    def predictProbability(self, value: Vector) -> Vector: ...

class _LinearSVCParams(
    _JavaClassifierParams,
    HasRegParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasStandardization,
    HasWeightCol,
    HasAggregationDepth,
    HasThreshold,
):
    threshold: Param[float]

class LinearSVC(
    JavaClassifier[LinearSVCModel],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable[LinearSVC],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        tol: float = ...,
        rawPredictionCol: str = ...,
        fitIntercept: bool = ...,
        standardization: bool = ...,
        threshold: float = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        tol: float = ...,
        rawPredictionCol: str = ...,
        fitIntercept: bool = ...,
        standardization: bool = ...,
        threshold: float = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...
    ) -> LinearSVC: ...
    def setMaxIter(self, value: int) -> LinearSVC: ...
    def setRegParam(self, value: float) -> LinearSVC: ...
    def setTol(self, value: float) -> LinearSVC: ...
    def setFitIntercept(self, value: bool) -> LinearSVC: ...
    def setStandardization(self, value: bool) -> LinearSVC: ...
    def setThreshold(self, value: float) -> LinearSVC: ...
    def setWeightCol(self, value: str) -> LinearSVC: ...
    def setAggregationDepth(self, value: int) -> LinearSVC: ...

class LinearSVCModel(
    JavaClassificationModel[Vector],
    _LinearSVCParams,
    JavaMLWritable,
    JavaMLReadable[LinearSVCModel],
):
    def setThreshold(self, value: float) -> LinearSVCModel: ...
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...

class _LogisticRegressionParams(
    _JavaProbabilisticClassifierParams,
    HasRegParam,
    HasElasticNetParam,
    HasMaxIter,
    HasFitIntercept,
    HasTol,
    HasStandardization,
    HasWeightCol,
    HasAggregationDepth,
    HasThreshold,
):
    threshold: Param[float]
    family: Param[str]
    lowerBoundsOnCoefficients: Param[Matrix]
    upperBoundsOnCoefficients: Param[Matrix]
    lowerBoundsOnIntercepts: Param[Vector]
    upperBoundsOnIntercepts: Param[Vector]
    def setThreshold(self: P, value: float) -> P: ...
    def getThreshold(self) -> float: ...
    def setThresholds(self: P, value: List[float]) -> P: ...
    def getThresholds(self) -> List[float]: ...
    def getFamily(self) -> str: ...
    def getLowerBoundsOnCoefficients(self) -> Matrix: ...
    def getUpperBoundsOnCoefficients(self) -> Matrix: ...
    def getLowerBoundsOnIntercepts(self) -> Vector: ...
    def getUpperBoundsOnIntercepts(self) -> Vector: ...

class LogisticRegression(
    JavaProbabilisticClassifier[LogisticRegressionModel],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable[LogisticRegression],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        threshold: float = ...,
        thresholds: Optional[List[float]] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        regParam: float = ...,
        elasticNetParam: float = ...,
        tol: float = ...,
        fitIntercept: bool = ...,
        threshold: float = ...,
        thresholds: Optional[List[float]] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        standardization: bool = ...,
        weightCol: Optional[str] = ...,
        aggregationDepth: int = ...,
        family: str = ...,
        lowerBoundsOnCoefficients: Optional[Matrix] = ...,
        upperBoundsOnCoefficients: Optional[Matrix] = ...,
        lowerBoundsOnIntercepts: Optional[Vector] = ...,
        upperBoundsOnIntercepts: Optional[Vector] = ...
    ) -> LogisticRegression: ...
    def setFamily(self, value: str) -> LogisticRegression: ...
    def setLowerBoundsOnCoefficients(self, value: Matrix) -> LogisticRegression: ...
    def setUpperBoundsOnCoefficients(self, value: Matrix) -> LogisticRegression: ...
    def setLowerBoundsOnIntercepts(self, value: Vector) -> LogisticRegression: ...
    def setUpperBoundsOnIntercepts(self, value: Vector) -> LogisticRegression: ...
    def setMaxIter(self, value: int) -> LogisticRegression: ...
    def setRegParam(self, value: float) -> LogisticRegression: ...
    def setTol(self, value: float) -> LogisticRegression: ...
    def setElasticNetParam(self, value: float) -> LogisticRegression: ...
    def setFitIntercept(self, value: bool) -> LogisticRegression: ...
    def setStandardization(self, value: bool) -> LogisticRegression: ...
    def setWeightCol(self, value: str) -> LogisticRegression: ...
    def setAggregationDepth(self, value: int) -> LogisticRegression: ...

class LogisticRegressionModel(
    JavaProbabilisticClassificationModel[Vector],
    _LogisticRegressionParams,
    JavaMLWritable,
    JavaMLReadable[LogisticRegressionModel],
    HasTrainingSummary[LogisticRegressionTrainingSummary],
):
    @property
    def coefficients(self) -> Vector: ...
    @property
    def intercept(self) -> float: ...
    @property
    def coefficientMatrix(self) -> Matrix: ...
    @property
    def interceptVector(self) -> Vector: ...
    @property
    def summary(self) -> LogisticRegressionTrainingSummary: ...
    @property
    def hasSummary(self) -> bool: ...
    def evaluate(self, dataset: DataFrame) -> LogisticRegressionSummary: ...

class LogisticRegressionSummary(JavaWrapper):
    @property
    def predictions(self) -> DataFrame: ...
    @property
    def probabilityCol(self) -> str: ...
    @property
    def predictionCol(self) -> str: ...
    @property
    def labelCol(self) -> str: ...
    @property
    def featuresCol(self) -> str: ...
    @property
    def labels(self) -> List[float]: ...
    @property
    def truePositiveRateByLabel(self) -> List[float]: ...
    @property
    def falsePositiveRateByLabel(self) -> List[float]: ...
    @property
    def precisionByLabel(self) -> List[float]: ...
    @property
    def recallByLabel(self) -> List[float]: ...
    def fMeasureByLabel(self, beta: float = ...) -> List[float]: ...
    @property
    def accuracy(self) -> float: ...
    @property
    def weightedTruePositiveRate(self) -> float: ...
    @property
    def weightedFalsePositiveRate(self) -> float: ...
    @property
    def weightedRecall(self) -> float: ...
    @property
    def weightedPrecision(self) -> float: ...
    def weightedFMeasure(self, beta: float = ...) -> float: ...

class LogisticRegressionTrainingSummary(LogisticRegressionSummary):
    @property
    def objectiveHistory(self) -> List[float]: ...
    @property
    def totalIterations(self) -> int: ...

class BinaryLogisticRegressionSummary(LogisticRegressionSummary):
    @property
    def roc(self) -> DataFrame: ...
    @property
    def areaUnderROC(self) -> float: ...
    @property
    def pr(self) -> DataFrame: ...
    @property
    def fMeasureByThreshold(self) -> DataFrame: ...
    @property
    def precisionByThreshold(self) -> DataFrame: ...
    @property
    def recallByThreshold(self) -> DataFrame: ...

class BinaryLogisticRegressionTrainingSummary(
    BinaryLogisticRegressionSummary, LogisticRegressionTrainingSummary
): ...
class _DecisionTreeClassifierParams(_DecisionTreeParams, _TreeClassifierParams): ...

class DecisionTreeClassifier(
    JavaProbabilisticClassifier[DecisionTreeClassificationModel],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        seed: Optional[int] = ...,
        weightCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        seed: Optional[int] = ...,
        weightCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...
    ) -> DecisionTreeClassifier: ...
    def setMaxDepth(self, value: int) -> DecisionTreeClassifier: ...
    def setMaxBins(self, value: int) -> DecisionTreeClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> DecisionTreeClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> DecisionTreeClassifier: ...
    def setMinInfoGain(self, value: float) -> DecisionTreeClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> DecisionTreeClassifier: ...
    def setCacheNodeIds(self, value: bool) -> DecisionTreeClassifier: ...
    def setImpurity(self, value: str) -> DecisionTreeClassifier: ...
    def setCheckpointInterval(self, value: int) -> DecisionTreeClassifier: ...
    def setSeed(self, value: int) -> DecisionTreeClassifier: ...
    def setWeightCol(self, value: str) -> DecisionTreeClassifier: ...

class DecisionTreeClassificationModel(
    _DecisionTreeModel,
    JavaProbabilisticClassificationModel[Vector],
    _DecisionTreeClassifierParams,
    JavaMLWritable,
    JavaMLReadable[DecisionTreeClassificationModel],
):
    @property
    def featureImportances(self) -> Vector: ...

class _RandomForestClassifierParams(_RandomForestParams, _TreeClassifierParams): ...

class RandomForestClassifier(
    JavaProbabilisticClassifier[RandomForestClassificationModel],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        impurity: str = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        seed: Optional[int] = ...,
        impurity: str = ...,
        numTrees: int = ...,
        featureSubsetStrategy: str = ...,
        subsamplingRate: float = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...,
        bootstrap: Optional[bool] = ...
    ) -> RandomForestClassifier: ...
    def setMaxDepth(self, value: int) -> RandomForestClassifier: ...
    def setMaxBins(self, value: int) -> RandomForestClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> RandomForestClassifier: ...
    def setMinInfoGain(self, value: float) -> RandomForestClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> RandomForestClassifier: ...
    def setCacheNodeIds(self, value: bool) -> RandomForestClassifier: ...
    def setImpurity(self, value: str) -> RandomForestClassifier: ...
    def setNumTrees(self, value: int) -> RandomForestClassifier: ...
    def setBootstrap(self, value: bool) -> RandomForestClassifier: ...
    def setSubsamplingRate(self, value: float) -> RandomForestClassifier: ...
    def setFeatureSubsetStrategy(self, value: str) -> RandomForestClassifier: ...
    def setSeed(self, value: int) -> RandomForestClassifier: ...
    def setCheckpointInterval(self, value: int) -> RandomForestClassifier: ...
    def setWeightCol(self, value: str) -> RandomForestClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> RandomForestClassifier: ...

class RandomForestClassificationModel(
    _TreeEnsembleModel,
    JavaProbabilisticClassificationModel[Vector],
    _RandomForestClassifierParams,
    JavaMLWritable,
    JavaMLReadable[RandomForestClassificationModel],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> List[DecisionTreeClassificationModel]: ...

class _GBTClassifierParams(_GBTParams, _HasVarianceImpurity):
    supportedLossTypes: List[str]
    lossType: Param[str]
    def getLossType(self) -> str: ...

class GBTClassifier(
    JavaProbabilisticClassifier[GBTClassificationModel],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable[GBTClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxDepth: int = ...,
        maxBins: int = ...,
        minInstancesPerNode: int = ...,
        minInfoGain: float = ...,
        maxMemoryInMB: int = ...,
        cacheNodeIds: bool = ...,
        checkpointInterval: int = ...,
        lossType: str = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        seed: Optional[int] = ...,
        subsamplingRate: float = ...,
        featureSubsetStrategy: str = ...,
        validationTol: float = ...,
        validationIndicatorCol: Optional[str] = ...,
        leafCol: str = ...,
        minWeightFractionPerNode: float = ...,
        weightCol: Optional[str] = ...
    ) -> GBTClassifier: ...
    def setMaxDepth(self, value: int) -> GBTClassifier: ...
    def setMaxBins(self, value: int) -> GBTClassifier: ...
    def setMinInstancesPerNode(self, value: int) -> GBTClassifier: ...
    def setMinInfoGain(self, value: float) -> GBTClassifier: ...
    def setMaxMemoryInMB(self, value: int) -> GBTClassifier: ...
    def setCacheNodeIds(self, value: bool) -> GBTClassifier: ...
    def setImpurity(self, value: str) -> GBTClassifier: ...
    def setLossType(self, value: str) -> GBTClassifier: ...
    def setSubsamplingRate(self, value: float) -> GBTClassifier: ...
    def setFeatureSubsetStrategy(self, value: str) -> GBTClassifier: ...
    def setValidationIndicatorCol(self, value: str) -> GBTClassifier: ...
    def setMaxIter(self, value: int) -> GBTClassifier: ...
    def setCheckpointInterval(self, value: int) -> GBTClassifier: ...
    def setSeed(self, value: int) -> GBTClassifier: ...
    def setStepSize(self, value: float) -> GBTClassifier: ...
    def setWeightCol(self, value: str) -> GBTClassifier: ...
    def setMinWeightFractionPerNode(self, value: float) -> GBTClassifier: ...

class GBTClassificationModel(
    _TreeEnsembleModel,
    JavaProbabilisticClassificationModel[Vector],
    _GBTClassifierParams,
    JavaMLWritable,
    JavaMLReadable[GBTClassificationModel],
):
    @property
    def featureImportances(self) -> Vector: ...
    @property
    def trees(self) -> List[DecisionTreeRegressionModel]: ...
    def evaluateEachIteration(self, dataset: DataFrame) -> List[float]: ...

class _NaiveBayesParams(_JavaPredictorParams, HasWeightCol):
    smoothing: Param[float]
    modelType: Param[str]
    def getSmoothing(self) -> float: ...
    def getModelType(self) -> str: ...

class NaiveBayes(
    JavaProbabilisticClassifier[NaiveBayesModel],
    _NaiveBayesParams,
    HasThresholds,
    HasWeightCol,
    JavaMLWritable,
    JavaMLReadable[NaiveBayes],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        smoothing: float = ...,
        modelType: str = ...,
        thresholds: Optional[List[float]] = ...,
        weightCol: Optional[str] = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        smoothing: float = ...,
        modelType: str = ...,
        thresholds: Optional[List[float]] = ...,
        weightCol: Optional[str] = ...
    ) -> NaiveBayes: ...
    def setSmoothing(self, value: float) -> NaiveBayes: ...
    def setModelType(self, value: str) -> NaiveBayes: ...
    def setWeightCol(self, value: str) -> NaiveBayes: ...

class NaiveBayesModel(
    JavaProbabilisticClassificationModel[Vector],
    _NaiveBayesParams,
    JavaMLWritable,
    JavaMLReadable[NaiveBayesModel],
):
    @property
    def pi(self) -> Vector: ...
    @property
    def theta(self) -> Matrix: ...
    @property
    def sigma(self) -> Matrix: ...

class _MultilayerPerceptronParams(
    _JavaProbabilisticClassifierParams,
    HasSeed,
    HasMaxIter,
    HasTol,
    HasStepSize,
    HasSolver,
    HasBlockSize,
):
    layers: Param[List[int]]
    solver: Param[str]
    initialWeights: Param[Vector]
    def getLayers(self) -> List[int]: ...
    def getInitialWeights(self) -> Vector: ...

class MultilayerPerceptronClassifier(
    JavaProbabilisticClassifier[MultilayerPerceptronClassificationModel],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable[MultilayerPerceptronClassifier],
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        tol: float = ...,
        seed: Optional[int] = ...,
        layers: Optional[List[int]] = ...,
        blockSize: int = ...,
        stepSize: float = ...,
        solver: str = ...,
        initialWeights: Optional[Vector] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        maxIter: int = ...,
        tol: float = ...,
        seed: Optional[int] = ...,
        layers: Optional[List[int]] = ...,
        blockSize: int = ...,
        stepSize: float = ...,
        solver: str = ...,
        initialWeights: Optional[Vector] = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...
    ) -> MultilayerPerceptronClassifier: ...
    def setLayers(self, value: List[int]) -> MultilayerPerceptronClassifier: ...
    def setBlockSize(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setInitialWeights(self, value: Vector) -> MultilayerPerceptronClassifier: ...
    def setMaxIter(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setSeed(self, value: int) -> MultilayerPerceptronClassifier: ...
    def setTol(self, value: float) -> MultilayerPerceptronClassifier: ...
    def setStepSize(self, value: float) -> MultilayerPerceptronClassifier: ...
    def setSolver(self, value: str) -> MultilayerPerceptronClassifier: ...

class MultilayerPerceptronClassificationModel(
    JavaProbabilisticClassificationModel[Vector],
    _MultilayerPerceptronParams,
    JavaMLWritable,
    JavaMLReadable[MultilayerPerceptronClassificationModel],
):
    @property
    def weights(self) -> Vector: ...

class _OneVsRestParams(_JavaClassifierParams, HasWeightCol):
    classifier: Param[Estimator]
    def getClassifier(self) -> Estimator[M]: ...

class OneVsRest(
    Estimator[OneVsRestModel],
    _OneVsRestParams,
    HasParallelism,
    JavaMLReadable[OneVsRest],
    JavaMLWritable,
):
    def __init__(
        self,
        *,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        rawPredictionCol: str = ...,
        classifier: Optional[Estimator[M]] = ...,
        weightCol: Optional[str] = ...,
        parallelism: int = ...
    ) -> None: ...
    def setParams(
        self,
        *,
        featuresCol: Optional[str] = ...,
        labelCol: Optional[str] = ...,
        predictionCol: Optional[str] = ...,
        rawPredictionCol: str = ...,
        classifier: Optional[Estimator[M]] = ...,
        weightCol: Optional[str] = ...,
        parallelism: int = ...
    ) -> OneVsRest: ...
    def setClassifier(self, value: Estimator[M]) -> OneVsRest: ...
    def setLabelCol(self, value: str) -> OneVsRest: ...
    def setFeaturesCol(self, value: str) -> OneVsRest: ...
    def setPredictionCol(self, value: str) -> OneVsRest: ...
    def setRawPredictionCol(self, value: str) -> OneVsRest: ...
    def setWeightCol(self, value: str) -> OneVsRest: ...
    def setParallelism(self, value: int) -> OneVsRest: ...
    def copy(self, extra: Optional[ParamMap] = ...) -> OneVsRest: ...

class OneVsRestModel(
    Model, _OneVsRestParams, JavaMLReadable[OneVsRestModel], JavaMLWritable
):
    models: List[Transformer]
    def __init__(self, models: List[Transformer]) -> None: ...
    def setFeaturesCol(self, value: str) -> OneVsRestModel: ...
    def setPredictionCol(self, value: str) -> OneVsRestModel: ...
    def setRawPredictionCol(self, value: str) -> OneVsRestModel: ...
    def copy(self, extra: Optional[ParamMap] = ...) -> OneVsRestModel: ...

class FMClassifier(
    JavaProbabilisticClassifier[FMClassificationModel],
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMClassifier],
):
    factorSize: Param[int]
    fitLinear: Param[bool]
    miniBatchFraction: Param[float]
    initStd: Param[float]
    solver: Param[str]
    def __init__(
        self,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        factorSize: int = ...,
        fitIntercept: bool = ...,
        fitLinear: bool = ...,
        regParam: float = ...,
        miniBatchFraction: float = ...,
        initStd: float = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        tol: float = ...,
        solver: str = ...,
        thresholds: Optional[Any] = ...,
        seed: Optional[Any] = ...,
    ) -> None: ...
    def setParams(
        self,
        featuresCol: str = ...,
        labelCol: str = ...,
        predictionCol: str = ...,
        probabilityCol: str = ...,
        rawPredictionCol: str = ...,
        factorSize: int = ...,
        fitIntercept: bool = ...,
        fitLinear: bool = ...,
        regParam: float = ...,
        miniBatchFraction: float = ...,
        initStd: float = ...,
        maxIter: int = ...,
        stepSize: float = ...,
        tol: float = ...,
        solver: str = ...,
        thresholds: Optional[Any] = ...,
        seed: Optional[Any] = ...,
    ): ...
    def setFactorSize(self, value: int) -> FMClassifier: ...
    def setFitLinear(self, value: bool) -> FMClassifier: ...
    def setMiniBatchFraction(self, value: float) -> FMClassifier: ...
    def setInitStd(self, value: float) -> FMClassifier: ...
    def setMaxIter(self, value: int) -> FMClassifier: ...
    def setStepSize(self, value: float) -> FMClassifier: ...
    def setTol(self, value: float) -> FMClassifier: ...
    def setSolver(self, value: str) -> FMClassifier: ...
    def setSeed(self, value: int) -> FMClassifier: ...
    def setFitIntercept(self, value: bool) -> FMClassifier: ...
    def setRegParam(self, value: float) -> FMClassifier: ...

class FMClassificationModel(
    JavaProbabilisticClassificationModel,
    _FactorizationMachinesParams,
    JavaMLWritable,
    JavaMLReadable[FMClassificationModel],
):
    @property
    def intercept(self) -> float: ...
    @property
    def linear(self) -> Vector: ...
    @property
    def factors(self) -> Matrix: ...
