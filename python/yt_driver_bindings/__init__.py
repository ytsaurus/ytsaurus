# TODO(asaitgalin): Remove try/except when DEVTOOLS-3781 is done.
try:
    from driver_lib import Driver, BufferedStream, Response, configure_logging, configure_tracing, configure_address_resolver, reopen_logs
except ImportError:
    from .driver_lib import Driver, BufferedStream, Response, configure_logging, configure_tracing, configure_address_resolver, reopen_logs

from .driver import Request
