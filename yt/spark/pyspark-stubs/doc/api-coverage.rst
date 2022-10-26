API Coverage
=============

+------------------------------------------------+---------------------+--------------------+------------+
| Module                                         | Dynamically typed   | Statically typed   | Notes      |
+================================================+=====================+====================+============+
| `pyspark`_                                     | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.accumulators`_                        | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.broadcast`_                           | ✔                   | ✔                  | Mixed      |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.cloudpickle                            | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.conf`_                                | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.context`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.daemon                                 | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.files`_                               | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.find\_spark\_home                      | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.heapq3                                 | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.java\_gateway                          | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.join`_                                | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml`_                                  | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.base`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.classification`_                   | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.clustering`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.common`_                           | ✔                   | ✔                  | Mixed      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.evaluation`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.feature`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.fpm`_                              | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.image`_                            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.linalg`_                           | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.param`_                            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.ml.param.\_shared\_params\_code\_gen   | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.param.shared`_                     | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.pipeline`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.recommendation`_                   | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.regression`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.stat`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.ml.tests                               | ✘                   | ✘                  | Tests      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.tree`_                             | x                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.tuning`_                           | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.util`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.ml.wrapper`_                          | ✔                   | ✔                  | Mixed      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib`_                               | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.classification`_                | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.clustering`_                    | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.common`_                        | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.evaluation`_                    | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.feature`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.fpm`_                           | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.linalg`_                        | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.linalg.distributed`_            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.random`_                        | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.recommendation`_                | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.regression`_                    | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.stat`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.stat.KernelDensity`_            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.stat.\_statistics`_             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.stat.distribution`_             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.stat.test`_                     | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.mllib.tests                            | ✘                   | ✘                  | Tests      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.tree`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.mllib.util`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.profiler`_                            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.resourceinformation`_                 | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.rdd`_                                 | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.rddsampler`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.resultiterable`_                      | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.serializers`_                         | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.shell                                  | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.shuffle                                | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql`_                                 | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.avro`_                            | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.avro.functions`_                  | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.catalog`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.column`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.conf`_                            | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.context`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.dataframe`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.functions`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.group`_                           | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas`_                          | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.conversion`_               | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.functions`_                | ✔                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.group\_ops`_               | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.map\_ops`_                 | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.serializers`_              | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.typehints`_                | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.types`_                    | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.pandas.utils`_                    | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.readwriter`_                      | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.session`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.streaming`_                       | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.sql.tests                              | ✘                   | ✘                  | Tests      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.types`_                           | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.udf`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.utils`_                           | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.sql.window`_                          | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.statcounter`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.status`_                              | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.storagelevel`_                        | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming`_                           | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming.context`_                   | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming.dstream`_                   | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming.kinesis`_                   | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming.listener`_                  | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.streaming.tests                        | ✘                   | ✘                  | Tests      |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.streaming.util`_                      | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.taskcontext`_                         | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.tests                                  | ✘                   | ✘                  | Tests      |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.traceback\_utils                       | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.util`_                                | ✔                   | ✘                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| `pyspark.version`_                             | ✘                   | ✔                  |            |
+------------------------------------------------+---------------------+--------------------+------------+
| pyspark.worker                                 | ✔                   | ✘                  | Internal   |
+------------------------------------------------+---------------------+--------------------+------------+



.. _pyspark: ../third_party/3/pyspark/__init__.pyi
.. _pyspark.accumulators: ../third_party/3/pyspark/accumulators.pyi
.. _pyspark.broadcast: ../third_party/3/pyspark/broadcast.pyi
.. _pyspark.conf: ../third_party/3/pyspark/conf.pyi
.. _pyspark.context: ../third_party/3/pyspark/context.pyi
.. _pyspark.files: ../third_party/3/pyspark/files.pyi
.. _pyspark.join: ../third_party/3/pyspark/join.pyi
.. _pyspark.ml: ../third_party/3/pyspark/ml/__init__.pyi
.. _pyspark.ml.base: ../third_party/3/pyspark/ml/base.pyi
.. _pyspark.ml.classification: ../third_party/3/pyspark/ml/classification.pyi
.. _pyspark.ml.clustering: ../third_party/3/pyspark/ml/clustering.pyi
.. _pyspark.ml.common: ../third_party/3/pyspark/ml/common.pyi
.. _pyspark.ml.evaluation: ../third_party/3/pyspark/ml/evaluation.pyi
.. _pyspark.ml.feature: ../third_party/3/pyspark/ml/feature.pyi
.. _pyspark.ml.fpm: ../third_party/3/pyspark/ml/fpm.pyi
.. _pyspark.ml.image: ../third_party/3/pyspark/ml/image.pyi
.. _pyspark.ml.linalg: ../third_party/3/pyspark/ml/linalg/__init__.pyi
.. _pyspark.ml.param: ../third_party/3/pyspark/ml/param/__init__.pyi
.. _pyspark.ml.param.shared: ../third_party/3/pyspark/ml/param/shared.pyi
.. _pyspark.ml.pipeline: ../third_party/3/pyspark/ml/pipeline.pyi
.. _pyspark.ml.recommendation: ../third_party/3/pyspark/ml/recommendation.pyi
.. _pyspark.ml.regression: ../third_party/3/pyspark/ml/regression.pyi
.. _pyspark.ml.stat: ../third_party/3/pyspark/ml/stat.pyi
.. _pyspark.ml.tree: ../third_party/3/pyspark/ml/tree.pyi
.. _pyspark.ml.tuning: ../third_party/3/pyspark/ml/tuning.pyi
.. _pyspark.ml.util: ../third_party/3/pyspark/ml/util.pyi
.. _pyspark.ml.wrapper: ../third_party/3/pyspark/ml/wrapper.pyi
.. _pyspark.mllib: ../third_party/3/pyspark/mllib/__init__.pyi
.. _pyspark.mllib.classification: ../third_party/3/pyspark/mllib/classification.pyi
.. _pyspark.mllib.clustering: ../third_party/3/pyspark/mllib/clustering.pyi
.. _pyspark.mllib.common: ../third_party/3/pyspark/mllib/common.pyi
.. _pyspark.mllib.evaluation: ../third_party/3/pyspark/mllib/evaluation.pyi
.. _pyspark.mllib.feature: ../third_party/3/pyspark/mllib/feature.pyi
.. _pyspark.mllib.fpm: ../third_party/3/pyspark/mllib/fpm.pyi
.. _pyspark.mllib.linalg: ../third_party/3/pyspark/mllib/linalg/__init__.pyi
.. _pyspark.mllib.linalg.distributed: ../third_party/3/pyspark/mllib/linalg/distributed.pyi
.. _pyspark.mllib.random: ../third_party/3/pyspark/mllib/random.pyi
.. _pyspark.mllib.recommendation: ../third_party/3/pyspark/mllib/recommendation.pyi
.. _pyspark.mllib.regression: ../third_party/3/pyspark/mllib/regression.pyi
.. _pyspark.mllib.stat: ../third_party/3/pyspark/mllib/stat/__init__.pyi
.. _pyspark.mllib.stat.KernelDensity: ../third_party/3/pyspark/mllib/stat/KernelDensity.pyi
.. _pyspark.mllib.stat._statistics: ../third_party/3/pyspark/mllib/stat/_statistics.pyi
.. _pyspark.mllib.stat.distribution: ../third_party/3/pyspark/mllib/stat/distribution.pyi
.. _pyspark.mllib.stat.test: ../third_party/3/pyspark/mllib/stat/test.pyi
.. _pyspark.mllib.tree: ../third_party/3/pyspark/mllib/tree.pyi
.. _pyspark.mllib.util: ../third_party/3/pyspark/mllib/util.pyi
.. _pyspark.profiler: ../third_party/3/pyspark/profiler.pyi
.. _pyspark.resourceinformation: ../third_party/3/pyspark/resourceinformation.pyi
.. _pyspark.rdd: ../third_party/3/pyspark/rdd.pyi
.. _pyspark.rddsampler: ../third_party/3/pyspark/rddsampler.pyi
.. _pyspark.resultiterable: ../third_party/3/pyspark/resultiterable.pyi
.. _pyspark.serializers: ../third_party/3/pyspark/serializers.pyi
.. _pyspark.sql: ../third_party/3/pyspark/sql/__init__.pyi
.. _pyspark.sql.avro: ../third_party/3/pyspark/sql/avro/__init__.pyi
.. _pyspark.sql.avro.functions: ../third_party/3/pyspark/sql/avro/functions.pyi
.. _pyspark.sql.catalog: ../third_party/3/pyspark/sql/catalog.pyi
.. _pyspark.sql.column: ../third_party/3/pyspark/sql/column.pyi
.. _pyspark.sql.conf: ../third_party/3/pyspark/sql/conf.pyi
.. _pyspark.sql.context: ../third_party/3/pyspark/sql/context.pyi
.. _pyspark.sql.dataframe: ../third_party/3/pyspark/sql/dataframe.pyi
.. _pyspark.sql.functions: ../third_party/3/pyspark/sql/functions.pyi
.. _pyspark.sql.group: ../third_party/3/pyspark/sql/group.pyi
.. _pyspark.sql.pandas: ../third_party/3/pyspark/sql/pandas/__init__.pyi
.. _pyspark.sql.pandas.conversion: ../third_party/3/pyspark/sql/pandas/conversion.pyi
.. _pyspark.sql.pandas.group_ops: ../third_party/3/pyspark/sql/pandas/group_ops.pyi
.. _pyspark.sql.pandas.map_ops: ../third_party/3/pyspark/sql/pandas/map_ops.pyi
.. _pyspark.sql.pandas.typehints: ../third_party/3/pyspark/sql/pandas/typehints.pyi
.. _pyspark.sql.pandas.types: ../third_party/3/pyspark/sql/pandas/types.pyi
.. _pyspark.sql.pandas.functions: ../third_party/3/pyspark/sql/pandas/functions.pyi
.. _pyspark.sql.pandas.serializers: ../third_party/3/pyspark/sql/pandas/serializers.pyi
.. _pyspark.sql.pandas.utils: ../third_party/3/pyspark/sql/pandas/utils.pyi
.. _pyspark.sql.readwriter: ../third_party/3/pyspark/sql/readwriter.pyi
.. _pyspark.sql.session: ../third_party/3/pyspark/sql/session.pyi
.. _pyspark.sql.streaming: ../third_party/3/pyspark/sql/streaming.pyi
.. _pyspark.sql.types: ../third_party/3/pyspark/sql/types.pyi
.. _pyspark.sql.udf: ../third_party/3/pyspark/sql/udf.pyi
.. _pyspark.sql.utils: ../third_party/3/pyspark/sql/utils.pyi
.. _pyspark.sql.window: ../third_party/3/pyspark/sql/window.pyi
.. _pyspark.statcounter: ../third_party/3/pyspark/statcounter.pyi
.. _pyspark.status: ../third_party/3/pyspark/status.pyi
.. _pyspark.storagelevel: ../third_party/3/pyspark/storagelevel.pyi
.. _pyspark.streaming: ../third_party/3/pyspark/streaming/__init__.pyi
.. _pyspark.streaming.context: ../third_party/3/pyspark/streaming/context.pyi
.. _pyspark.streaming.dstream: ../third_party/3/pyspark/streaming/dstream.pyi
.. _pyspark.streaming.kinesis: ../third_party/3/pyspark/streaming/kinesis.pyi
.. _pyspark.streaming.listener: ../third_party/3/pyspark/streaming/listener.pyi
.. _pyspark.streaming.util: ../third_party/3/pyspark/streaming/util.pyi
.. _pyspark.taskcontext: ../third_party/3/pyspark/taskcontext.pyi
.. _pyspark.util: ../third_party/3/pyspark/util.pyi
.. _pyspark.version: ../third_party/3/pyspark/version.pyi
