from pyspark.sql import functions as f, types as t
from spyt import spark_session
from pyspark.sql import functions as f, DataFrame, Column
from spyt.types import YsonType


from typing import List, Tuple, Union, Iterable, Callable


from dataclasses import dataclass

test_name = "DmCommutationCheckNewbieCheck"
input_path = "//home/spark/e2e/{}/input".format(test_name)
output_path = "//home/spark/e2e/{}/output".format(test_name)
print(test_name)

SPARK_DATE_FORMAT = 'yyyy-MM-dd'

def build_doc_column(df, skip_columns=None):
    if skip_columns is None:
        skip_columns = []
    columns = [df[column] for column in df.columns if column not in skip_columns]
    return f.struct(*columns)

class BaseCheckTask:
    def __init__(self,
                 spark,
                 name: str,
                 check_id: str,
                 check_root_id: str,
                 old_results_path: str,
                 current_results_path: str,
                 output_path: str,
                 source_transform=lambda x: x,
                 ):
        self._info_cols = []
        self.info_cols(
            ('details', lambda src: build_doc_column(src)),
        )
        self.name = name
        self.check_id = check_id
        self.check_root_id = check_root_id
        self.old_results_path = old_results_path
        self.current_results_path = current_results_path
        self.output_path = output_path
        self.spark = spark

    def info_cols(self, *cols):
        self._info_cols.clear()
        for col_name, col_expr in cols:
            self._info_cols.append((col_name, col_expr))

    def run_check(self):
        msk_updated_dttm = "2022-01-17 12:10:00"

        spark = self.spark

        from spyt.common import col_to_yson

        error_store = spark.read.yt(self.old_results_path)
        error_source = spark.read.yt(self.current_results_path)

        other_errors = (
            error_store
                .filter(f.col('check_root_id') != self.check_root_id)
        )

        current_errors = (
            error_source
                .select(
                    f.lit(self.name).alias('check_name'),
                    f.lit(self.check_id).alias('check_id'),
                    f.lit(self.check_root_id).alias('check_root_id'),
                    f.lit(msk_updated_dttm).alias('msk_updated_dttm'),
                    f.col('error_cnt'),
                    *[
                        col_expr(error_source).alias(col_name)
                        for col_name, col_expr in self._info_cols
                    ]
                )
        )

        for col_name, col_type in current_errors.dtypes:
            if col_type.startswith('struct'):
                current_errors = (
                    col_to_yson(current_errors, col_name)
                )

        updated_error_store = (
            other_errors.unionByName(current_errors)
                .sort(f.asc('msk_updated_dttm'))
        )

        updated_error_store.write.mode("overwrite").optimize_for("scan").yt(self.output_path)



with spark_session() as spark:
    input_base = f"//home/spark/e2e/{test_name}/input"
    output_base = f"//home/spark/e2e/{test_name}"

    check = BaseCheckTask(spark,
                          name="DmCommutationCheckNewbieCheck",
                          check_id="0000-0000-0000-0000",
                          check_root_id="0000-0000-0000-0000",
                          old_results_path=input_base + "/old_results",
                          current_results_path=input_base + "/current_results",
                          output_path=output_base + "/output",
                          )
    check.run_check()
