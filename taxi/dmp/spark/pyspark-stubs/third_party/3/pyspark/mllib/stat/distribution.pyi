# Stubs for pyspark.mllib.stat.distribution (Python 3)
#

from typing import NamedTuple

from pyspark.mllib.linalg import Vector, Matrix

class MultivariateGaussian(NamedTuple):
    mu: Vector
    sigma: Matrix
