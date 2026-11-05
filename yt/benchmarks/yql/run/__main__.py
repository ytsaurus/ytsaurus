from . import common

################################################################################

try:
    from . import qt_run # noqa
except ImportError:
    pass

try:
    from . import yql_run # noqa
except ImportError:
    pass

from . import upload_metrics # noqa

################################################################################

if __name__ == "__main__":
    common.cli()
