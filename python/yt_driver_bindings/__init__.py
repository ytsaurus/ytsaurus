# TODO(asaitgalin): Remove try/except when DEVTOOLS-3781 is done.
try:
    from driver_lib import Driver, BufferedStream, Response, configure_logging, configure_tracing, configure_address_resolver
except ImportError:
    from .driver_lib import Driver, BufferedStream, Response, configure_logging, configure_tracing, configure_address_resolver

from .driver import Request
