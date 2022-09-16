from ..errors import YtError
from ..skiff import check_skiff_bindings

try:
    from yt.packages.six import PY3
except ImportError:
    from six import PY3


def _get_availability_error(py3=True, skiff=True):
    errors = []
    if py3 and not PY3:
        errors.append(YtError("This functionality works only in Python 3"))
    if skiff:
        try:
            check_skiff_bindings()
        except YtError as error:
            errors.append(error)
    if len(errors) > 0:
        return YtError(
            '"yt.wrapper.schema" module is not available',
            inner_errors=errors,
        )
    else:
        return None


def check_schema_module_available(**kwargs):
    error = _get_availability_error(**kwargs)
    if error is not None:
        raise error


def is_schema_module_available(**kwargs):
    return _get_availability_error(**kwargs) is None
