from mypy.test.data import DataDrivenTestCase, DataSuite
from mypy.test.testcheck import TypeCheckSuite


TypeCheckSuite.files = [
        "context.test",
        "ml-classification.test",
        "ml-evaluation.test",
        "ml-feature.test",
        "ml-param.test",
        "ml-readable.test",
        "resultiterable.test",
        "sql-readwriter.test",
        "sql-udf.test",
    ]
