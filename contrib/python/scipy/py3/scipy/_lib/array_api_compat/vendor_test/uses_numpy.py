# Basic test that vendoring works

from .vendored._compat import numpy as np_compat

import numpy as np

def _test_numpy():
    a = np_compat.asarray([1., 2., 3.])
    b = np_compat.arange(3, dtype=np_compat.float32)

    # np.pow does not exist. Update this to use something else if it is added
    res = np_compat.pow(a, b)
    assert res.dtype == np_compat.float64 == np.float64
    assert isinstance(a, np.ndarray)
    assert isinstance(b, np.ndarray)
    assert isinstance(res, np.ndarray)

    np.testing.assert_allclose(res, [1., 2., 9.])
