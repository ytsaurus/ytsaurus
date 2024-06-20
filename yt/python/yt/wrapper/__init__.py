"""
Python wrapper for HTTP-interface of YT system.

Package supports `YT API <https://ytsaurus.tech/docs/en/api/python/start>`_.

Be ready to catch :class:`YtError <yt.common.YtError>` after all commands!
"""

from . import version_check  # noqa

try:
    from .idm_client import YtIdmClient  # noqa
except ImportError:
    pass

from .client_api import *  # noqa
from .client import YtClient, create_client_with_command_params  # noqa

from .spec_builders import (  # noqa
    JobIOSpecBuilder, PartitionJobIOSpecBuilder, SortJobIOSpecBuilder, MergeJobIOSpecBuilder, ReduceJobIOSpecBuilder,
    MapJobIOSpecBuilder,
    UserJobSpecBuilder, TaskSpecBuilder, MapperSpecBuilder, ReducerSpecBuilder, ReduceCombinerSpecBuilder,
    ReduceSpecBuilder, JoinReduceSpecBuilder, MapSpecBuilder, MapReduceSpecBuilder, MergeSpecBuilder,
    SortSpecBuilder, RemoteCopySpecBuilder, EraseSpecBuilder, VanillaSpecBuilder)
from .errors import (  # noqa
    YtError, YtOperationFailedError, YtResponseError, YtHttpResponseError,
    YtProxyUnavailable, YtTokenError, YtTransactionPingError, YtRequestTimedOut)
from .batch_execution import YtBatchRequestFailedError  # noqa
from .yamr_record import Record  # noqa
from .format import (  # noqa
    DsvFormat, YamrFormat, YsonFormat, JsonFormat, SchemafulDsvFormat, SkiffFormat,
    YamredDsvFormat, Format, create_format, dumps_row, loads_row, YtFormatError, create_table_switch)
from .ypath import YPath, TablePath, FilePath, ypath_join, ypath_dirname, ypath_split, escape_ypath_literal  # noqa
from .operation_commands import format_operation_stderrs, Operation  # noqa
from .prepare_operation import TypedJob  # noqa
from .operations_tracker import OperationsTracker, OperationsTrackerPool  # noqa
from .py_wrapper import (  # noqa
    aggregator, raw, raw_io, reduce_aggregator,
    enable_python_job_processing_for_standalone_binary, initialize_python_job_processing,
    with_context, with_skiff_schemas, respawn_in_docker)
from .schema import yt_dataclass, create_yt_enum, OutputRow  # noqa
from .string_iter_io import StringIterIO  # noqa
from .user_statistics import write_statistics  # noqa
from .yamr_mode import set_yamr_mode  # noqa
from .dynamic_table_commands import ASYNC_LAST_COMMITTED_TIMESTAMP, SYNC_LAST_COMMITTED_TIMESTAMP  # noqa
# COMPAT(ignat)
from .dynamic_table_commands import ASYNC_LAST_COMMITED_TIMESTAMP, SYNC_LAST_COMMITED_TIMESTAMP  # noqa
from .skiff import convert_to_skiff_schema  # noqa
from .http_helpers import get_retriable_errors  # noqa

from .common import get_version, is_inside_job, escape_c  # noqa
__version__ = VERSION = get_version()

# Some useful parts of private API.
from .http_helpers import (  # noqa
    _cleanup_http_session,
    get_token as _get_token,
    get_proxy_url as _get_proxy_url,
    make_request_with_retries as _make_http_request_with_retries)

from . import schema  # noqa

# For PyCharm checks
from . import config  # noqa
from . import default_config  # noqa
from .config import update_config  # noqa
