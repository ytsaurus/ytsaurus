# 1.4 (2023-09-13)

## Major Changes

- Releases are now made with GitHub Actions (thanks
  [@matthewfeickert](https://github.com/matthewfeickert)).

## Minor Changes

- Fix `torch.result_type()` cross-kind promotion
  ([@lucascolley](https://github.com/lucascolley)).

- Fix the torch.take() wrapper to make axis optional for ndim = 1.

- Add requires-python metadata to the package
  ([@matthewfeickert](https://github.com/matthewfeickert)).

# 1.3 (2023-06-20)

## Major Changes

- Add [2022.12](https://data-apis.org/array-api/2022.12/) standard support.
  This includes things like adding complex dtype support, adding the new
  `take` function, and various minor changes in the specification.

## Minor Changes

- Support `"cpu"` in CuPy `to_device()`.

- Return a new array in NumPy/CuPy `reshape(copy=False)`.

- Fix signatures for PyTorch `broadcast_to` and `permute_dims`.

# 1.2 (2023-04-03)

## Major Changes

- Support the linalg extension in the `array_api_compat.torch` namespace.

- Add `isdtype()`.

## Minor Changes

- Fix the `k` keyword argument to `tril` and `triu` in `torch`.

# 1.1.1 (2023-03-10)

## Major Changes

- Rename `get_namespace()` to `array_namespace()` (`get_namespace()` is
  maintained as a backwards compatible alias).

## Minor Changes

- The minimum supported NumPy version is now 1.21. Fixed a few issues with
  NumPy 1.21 (with `unique_*` and `asarray`), although there are also a few
  known issues with this version (see the README).

- Add `api_version` to `get_namespace()`.

- `array_namespace()` (*née* `get_namespace()`) now works correctly with
  `torch` tensors.

- `array_namespace()` (*née* `get_namespace()`) now works correctly with
  `numpy.array_api` arrays.

- `array_namespace()` (*née* `get_namespace()`) now raises `TypeError` instead
  of `ValueError`.

- Fix the `torch.std` wrapper.

- Add `torch` wrappers for `ones`, `empty`, and `zeros` so that `shape` can be
  passed as a keyword argument.

# 1.1 (2023-02-24)

## Major Changes

- Added support for PyTorch.

- Add helper function `size()` (required if torch is used as
  `torch.Tensor.size` is a method that is incompatible with the array API
  [`.size`](https://data-apis.org/array-api/latest/API_specification/generated/array_api.array.size.html#array_api.array.size)).

- All wrapper functions that wrap existing library functions now pass through
  arbitrary `**kwargs`.

## Minor Changes

- Added CI to run against the [array API testsuite](https://github.com/data-apis/array-api-tests).

- Fix `sort(stable=False)` and `argsort(stable=False)` with CuPy.

# 1.0 (2022-12-05)

## Major Changes

- Initial release. Includes support for NumPy and CuPy.
