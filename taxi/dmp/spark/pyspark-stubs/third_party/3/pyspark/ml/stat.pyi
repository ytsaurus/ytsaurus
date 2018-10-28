from pyspark.sql.dataframe import DataFrame

class ChiSquareTest:
    @staticmethod
    def test(dataset: DataFrame, featuresCol: str, labelCol: str) -> DataFrame: ...

class Correlation:
    @staticmethod
    def corr(dataset: DataFrame, column: str, method: str = ...) -> DataFrame: ...
