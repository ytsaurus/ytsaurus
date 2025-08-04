# Array API compatibility library

This is a small wrapper around common array libraries that is compatible with
the [Array API standard](https://data-apis.org/array-api/latest/). Currently,
NumPy, CuPy, and PyTorch are supported. If you want support for other array
libraries, or if you encounter any issues, please [open an
issue](https://github.com/data-apis/array-api-compat/issues).

Note that some of the functionality in this library is backwards incompatible
with the corresponding wrapped libraries. The end-goal is to eventually make
each array library itself fully compatible with the array API, but this
requires making backwards incompatible changes in many cases, so this will
take some time.

Currently all libraries here are implemented against the [2022.12
version](https://data-apis.org/array-api/2022.12/) of the standard.

## Install

`array-api-compat` is available on both [PyPI](https://pypi.org/project/array-api-compat/)

```
python -m pip install array-api-compat
```

and [Conda-forge](https://anaconda.org/conda-forge/array-api-compat)

```
conda install --channel conda-forge array-api-compat
```

## Usage

The typical usage of this library will be to get the corresponding array API
compliant namespace from the input arrays using `array_namespace()`, like

```py
def your_function(x, y):
    xp = array_api_compat.array_namespace(x, y)
    # Now use xp as the array library namespace
    return xp.mean(x, axis=0) + 2*xp.std(y, axis=0)
```

If you wish to have library-specific code-paths, you can import the
corresponding wrapped namespace for each library, like

```py
import array_api_compat.numpy as np
```

```py
import array_api_compat.cupy as cp
```

```py
import array_api_compat.torch as torch
```

Each will include all the functions from the normal NumPy/CuPy/PyTorch
namespace, except that functions that are part of the array API are wrapped so
that they have the correct array API behavior. In each case, the array object
used will be the same array object from the wrapped library.

## Difference between `array_api_compat` and `numpy.array_api`

`numpy.array_api` is a strict minimal implementation of the Array API (see
[NEP 47](https://numpy.org/neps/nep-0047-array-api-standard.html)). For
example, `numpy.array_api` does not include any functions that are not part of
the array API specification, and will explicitly disallow behaviors that are
not required by the spec (e.g., [cross-kind type
promotions](https://data-apis.org/array-api/latest/API_specification/type_promotion.html)).
(`cupy.array_api` is similar to `numpy.array_api`)

`array_api_compat`, on the other hand, is just an extension of the
corresponding array library namespaces with changes needed to be compliant
with the array API. It includes all additional library functions not mentioned
in the spec, and allows any library behaviors not explicitly disallowed by it,
such as cross-kind casting.

In particular, unlike `numpy.array_api`, this package does not use a separate
`Array` object, but rather just uses the corresponding array library array
objects (`numpy.ndarray`, `cupy.ndarray`, `torch.Tensor`, etc.) directly. This
is because those are the objects that are going to be passed as inputs to
functions by end users. This does mean that a few behaviors cannot be wrapped
(see below), but most of the array API functional, so this does not affect
most things.

Array consuming library authors coding against the array API may wish to test
against `numpy.array_api` to ensure they are not using functionality outside
of the standard, but prefer this implementation for the default behavior for
end-users.

## Helper Functions

In addition to the wrapped library namespaces and functions in the array API
specification, there are several helper functions included here that aren't
part of the specification but which are useful for using the array API:

- `is_array_api_obj(x)`: Return `True` if `x` is an array API compatible array
  object.

- `array_namespace(*xs)`: Get the corresponding array API namespace for the
  arrays `xs`. For example, if the arrays are NumPy arrays, the returned
  namespace will be `array_api_compat.numpy`. Note that this function will
  also work for namespaces that aren't supported by this compat library but
  which do support the array API (i.e., arrays that have the
  `__array_namespace__` attribute).

- `device(x)`: Equivalent to
  [`x.device`](https://data-apis.org/array-api/latest/API_specification/generated/signatures.array_object.array.device.html)
  in the array API specification. Included because `numpy.ndarray` does not
  include the `device` attribute and this library does not wrap or extend the
  array object. Note that for NumPy, `device(x)` is always `"cpu"`.

- `to_device(x, device, /, *, stream=None)`: Equivalent to
  [`x.to_device`](https://data-apis.org/array-api/latest/API_specification/generated/signatures.array_object.array.to_device.html).
  Included because neither NumPy's, CuPy's, nor PyTorch's array objects
  include this method. For NumPy, this function effectively does nothing since
  the only supported device is the CPU, but for CuPy, this method supports
  CuPy CUDA
  [Device](https://docs.cupy.dev/en/stable/reference/generated/cupy.cuda.Device.html)
  and
  [Stream](https://docs.cupy.dev/en/stable/reference/generated/cupy.cuda.Stream.html)
  objects. For PyTorch, this is the same as
  [`x.to(device)`](https://pytorch.org/docs/stable/generated/torch.Tensor.to.html)
  (the `stream` argument is not supported in PyTorch).

- `size(x)`: Equivalent to
  [`x.size`](https://data-apis.org/array-api/latest/API_specification/generated/array_api.array.size.html#array_api.array.size),
  i.e., the number of elements in the array. Included because PyTorch's
  `Tensor` defines `size` as a method which returns the shape, and this cannot
  be wrapped because this compat library doesn't wrap or extend the array
  objects.

## Known Differences from the Array API Specification

There are some known differences between this library and the array API
specification:

### NumPy and CuPy

- The array methods `__array_namespace__`, `device` (for NumPy), `to_device`,
  and `mT` are not defined. This reuses `np.ndarray` and `cp.ndarray` and we
  don't want to monkeypatch or wrap it. The helper functions `device()` and
  `to_device()` are provided to work around these missing methods (see above).
  `x.mT` can be replaced with `xp.linalg.matrix_transpose(x)`.
  `array_namespace(x)` should be used instead of `x.__array_namespace__`.

- Value-based casting for scalars will be in effect unless explicitly disabled
  with the environment variable `NPY_PROMOTION_STATE=weak` or
  `np._set_promotion_state('weak')` (requires NumPy 1.24 or newer, see [NEP
  50](https://numpy.org/neps/nep-0050-scalar-promotion.html) and
  https://github.com/numpy/numpy/issues/22341)

- `asarray()` does not support `copy=False`.

- Functions which are not wrapped may not have the same type annotations
  as the spec.

- Functions which are not wrapped may not use positional-only arguments.

The minimum supported NumPy version is 1.21. However, this older version of
NumPy has a few issues:

- `unique_*` will not compare nans as unequal.
- `finfo()` has no `smallest_normal`.
- No `from_dlpack` or `__dlpack__`.
- `argmax()` and `argmin()` do not have `keepdims`.
- `qr()` doesn't support matrix stacks.
- `asarray()` doesn't support `copy=True` (as noted above, `copy=False` is not
  supported even in the latest NumPy).
- Type promotion behavior will be value based for 0-D arrays (and there is no
  `NPY_PROMOTION_STATE=weak` to disable this).

If any of these are an issue, it is recommended to bump your minimum NumPy
version.

### PyTorch

- Like NumPy/CuPy, we do not wrap the `torch.Tensor` object. It is missing the
  `__array_namespace__` and `to_device` methods, so the corresponding helper
  functions `array_namespace()` and `to_device()` in this library should be
  used instead (see above).

- The `x.size` attribute on `torch.Tensor` is a function that behaves
  differently from
  [`x.size`](https://data-apis.org/array-api/draft/API_specification/generated/array_api.array.size.html)
  in the spec. Use the `size(x)` helper function as a portable workaround (see
  above).

- PyTorch does not have unsigned integer types other than `uint8`, and no
  attempt is made to implement them here.

- PyTorch has type promotion semantics that differ from the array API
  specification for 0-D tensor objects. The array functions in this wrapper
  library do work around this, but the operators on the Tensor object do not,
  as no operators or methods on the Tensor object are modified. If this is a
  concern, use the functional form instead of the operator form, e.g., `add(x,
  y)` instead of `x + y`.

- [`unique_all()`](https://data-apis.org/array-api/latest/API_specification/generated/array_api.unique_all.html#array_api.unique_all)
  is not implemented, due to the fact that `torch.unique` does not support
  returning the `indices` array. The other
  [`unique_*`](https://data-apis.org/array-api/latest/API_specification/set_functions.html)
  functions are implemented.

- Slices do not support negative steps.

- [`std()`](https://data-apis.org/array-api/latest/API_specification/generated/array_api.std.html#array_api.std)
  and
  [`var()`](https://data-apis.org/array-api/latest/API_specification/generated/array_api.var.html#array_api.var)
  do not support floating-point `correction`.

- The `stream` argument of the `to_device()` helper (see above) is not
  supported.

- As with NumPy, type annotations and positional-only arguments may not
  exactly match the spec for functions that are not wrapped at all.

The minimum supported PyTorch version is 1.13.

## Vendoring

This library supports vendoring as an installation method. To vendor the
library, simply copy `array_api_compat` into the appropriate place in the
library, like

```
cp -R array_api_compat/ mylib/vendored/array_api_compat
```

You may also rename it to something else if you like (nowhere in the code
references the name "array_api_compat").

Alternatively, the library may be installed as dependency on PyPI.

## Implementation Notes

As noted before, the goal of this library is to reuse the NumPy and CuPy array
objects, rather than wrapping or extending them. This means that the functions
need to accept and return `np.ndarray` for NumPy and `cp.ndarray` for CuPy.

Each namespace (`array_api_compat.numpy`, `array_api_compat.cupy`, and
`array_api_compat.torch`) is populated with the normal library namespace (like
`from numpy import *`). Then specific functions are replaced with wrapped
variants.

Since NumPy and CuPy are nearly identical in behavior, most wrapping logic can
be shared between them. Wrapped functions that have the same logic between
NumPy and CuPy are in `array_api_compat/common/`.
These functions are defined like

```py
# In array_api_compat/common/_aliases.py

def acos(x, /, xp):
    return xp.arccos(x)
```

The `xp` argument refers to the original array namespace (either `numpy` or
`cupy`). Then in the specific `array_api_compat/numpy/` and
`array_api_compat/cupy/` namespaces, the `@get_xp` decorator is applied to
these functions, which automatically removes the `xp` argument from the
function signature and replaces it with the corresponding array library, like

```py
# In array_api_compat/numpy/_aliases.py

from ..common import _aliases

import numpy as np

acos = get_xp(np)(_aliases.acos)
```

This `acos` now has the signature `acos(x, /)` and calls `numpy.arccos`.

Similarly, for CuPy:

```py
# In array_api_compat/cupy/_aliases.py

from ..common import _aliases

import cupy as cp

acos = get_xp(cp)(_aliases.acos)
```

Since NumPy and CuPy are nearly identical in their behaviors, this allows
writing the wrapping logic for both libraries only once.

PyTorch uses a similar layout in `array_api_compat/torch/`, but it differs
enough from NumPy/CuPy that very few common wrappers for those libraries are
reused.

See https://numpy.org/doc/stable/reference/array_api.html for a full list of
changes from the base NumPy (the differences for CuPy are nearly identical). A
corresponding document does not yet exist for PyTorch, but you can examine the
various comments in the
[implementation](https://github.com/data-apis/array-api-compat/blob/main/array_api_compat/torch/_aliases.py)
to see what functions and behaviors have been wrapped.
