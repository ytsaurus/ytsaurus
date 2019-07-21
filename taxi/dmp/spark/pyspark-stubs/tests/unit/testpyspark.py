from mypy.test.data import DataDrivenTestCase, DataSuite
from mypy.test.testcheck import TypeCheckSuite


class PySparkCoreSuite(TypeCheckSuite):
    TypeCheckSuite.files = ["context.test"]
    required_out_section = True
