from mypy.test.data import DataDrivenTestCase, DataSuite
from mypy.test.testcheck import TypeCheckSuite


TypeCheckSuite.files = [
    "core-context.test",
    "core-rdd.test",
    "ml-classification.test",
    "ml-evaluation.test",
    "ml-feature.test",
    "ml-param.test",
    "ml-readable.test",
    "ml-regression.test",
    "resultiterable.test",
    "sql-column.test",
    "sql-dataframe.test",
    "sql-pandas-typing-compatibility.test",
    "sql-readwriter.test",
    "sql-session.test",
    "sql-udf.test",
]
