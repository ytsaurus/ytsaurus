# Basic test that vendoring works

from .vendored._compat import torch as torch_compat

import torch

def _test_torch():
    a = torch_compat.asarray([1., 2., 3.])
    b = torch_compat.arange(3, dtype=torch_compat.float64)
    assert a.dtype == torch_compat.float32 == torch.float32
    assert b.dtype == torch_compat.float64 == torch.float64

    # torch.expand_dims does not exist. Update this to use something else if it is added
    res = torch_compat.expand_dims(a, axis=0)
    assert res.dtype == torch_compat.float32 == torch.float32
    assert res.shape == (1, 3)
    assert isinstance(res.shape, torch.Size)
    assert isinstance(a, torch.Tensor)
    assert isinstance(b, torch.Tensor)
    assert isinstance(res, torch.Tensor)

    torch.testing.assert_allclose(res, [[1., 2., 3.]])
