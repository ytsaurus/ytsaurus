from ..errors import YtError
from ..skiff import check_skiff_bindings

from yt.packages.six import PY3

try:
    import yandex.type_info.typing  # noqa
    _TI_AVAILABLE = True
except ImportError:
    _TI_AVAILABLE = False


def _get_availability_error(py3=True, skiff=True, check_ti=True):
    errors = []
    if py3 and not PY3:
        errors.append(YtError("This functionality works only in Python 3"))
    if skiff:
        try:
            check_skiff_bindings()
        except YtError as error:
            errors.append(error)
    if check_ti and not _TI_AVAILABLE:
        errors.append(YtError(
            '"yandex-type-info" package is required to work with schemas. '
            'It is shipped as additional package and '
            'can be installed as pip package "yandex-type-info"'
        ))
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
