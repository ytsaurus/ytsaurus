"""YT usage errors"""

from .common import hide_auth_headers_in_request_info, hide_auth_headers

import yt.common

from yt.common import YtError, YtResponseError, PrettyPrintableDict, get_value

from yt.yson import yson_to_json


class YtOperationFailedError(YtError):
    """Operation failed during waiting completion."""
    def __init__(self, id, state, error, stderrs, url):
        message = "Operation {0} {1}".format(id, state)
        attributes = {
            "id": id,
            "state": state,
            "stderrs": stderrs,
            "url": url}

        inner_errors = []
        if error is not None:
            inner_errors.append(error)

        # TODO(ignat): Add all stderr as suberrors?
        if stderrs:
            failed_job = stderrs[0]
            failed_job_error = failed_job["error"]
            if "stderr" in failed_job:
                failed_job_error["attributes"]["stderr"] = failed_job["stderr"]
            inner_errors.append(failed_job_error)

        super(YtOperationFailedError, self).__init__(message, attributes=attributes, inner_errors=inner_errors)


# COMPAT
YtHttpResponseError = yt.common.YtResponseError


def create_response_error(underlying_error):
    sample_error = YtResponseError(underlying_error)
    if sample_error.is_request_queue_size_limit_exceeded():
        error = YtRequestQueueSizeLimitExceeded(underlying_error)
    elif sample_error.is_master_disconnected():
        error = YtMasterDisconnectedError(underlying_error)
    elif sample_error.is_concurrent_operations_limit_reached():
        error = YtConcurrentOperationsLimitExceeded(underlying_error)
    elif sample_error.is_request_timed_out():
        error = YtRequestTimedOut(underlying_error)
    elif sample_error.is_no_such_transaction():
        error = YtNoSuchTransaction(underlying_error)
    elif sample_error.is_account_limit_exceeded():
        error = YtAccountLimitExceeded(underlying_error)
    elif sample_error.is_master_communication_error():
        error = YtMasterCommunicationError(underlying_error)
    elif sample_error.is_chunk_unavailable():
        error = YtChunkUnavailable(underlying_error)
    elif sample_error.is_concurrent_transaction_lock_conflict():
        error = YtConcurrentTransactionLockConflict(underlying_error)
    elif sample_error.is_tablet_transaction_lock_conflict():
        error = YtTabletTransactionLockConflict(underlying_error)
    elif sample_error.is_cypress_transaction_lock_conflict():
        error = YtCypressTransactionLockConflict(underlying_error)
    elif sample_error.is_tablet_in_intermediate_state():
        error = YtTabletIsInIntermediateState(underlying_error)
    elif sample_error.is_no_such_tablet():
        error = YtNoSuchTablet(underlying_error)
    elif sample_error.is_tablet_not_mounted():
        error = YtTabletNotMounted(underlying_error)
    elif sample_error.is_rpc_unavailable():
        error = YtRpcUnavailable(underlying_error)
    elif sample_error.is_all_target_nodes_failed():
        error = YtAllTargetNodesFailed(underlying_error)
    elif sample_error.is_transport_error():
        error = YtTransportError(underlying_error)
    elif sample_error.is_retriable_archive_error():
        error = YtRetriableArchiveError(underlying_error)
    elif sample_error.is_resolve_error():
        error = YtResolveError(underlying_error)
    elif sample_error.is_row_is_blocked():
        error = YtRowIsBlocked(underlying_error)
    elif sample_error.is_blocked_row_wait_timeout():
        error = YtBlockedRowWaitTimeout(underlying_error)
    elif sample_error.is_no_such_cell():
        error = YtNoSuchCell(underlying_error)
    elif sample_error.is_chunk_not_preloaded():
        error = YtChunkNotPreloaded(underlying_error)
    elif sample_error.is_no_in_sync_replicas():
        error = YtNoInSyncReplicas(underlying_error)
    else:
        error = sample_error
    return error


def create_http_response_error(underlying_error, url, request_headers, response_headers, params):
    error = create_response_error(underlying_error)
    error.message = "Received HTTP response with error"
    attributes = hide_auth_headers_in_request_info({
        "url": url,
        "request_headers": PrettyPrintableDict(get_value(request_headers, {})),
        "response_headers": PrettyPrintableDict(get_value(response_headers, {})),
        "params": PrettyPrintableDict(yson_to_json(get_value(params, {}))),
        "transparent": True})
    error.attributes.update(attributes)
    return error


class YtRequestQueueSizeLimitExceeded(YtResponseError):
    """Request queue size limit exceeded error.
       It is used in retries.
    """
    pass


class YtRpcUnavailable(YtResponseError):
    """Rpc unavailable error.
       It is used in retries.
    """
    pass


class YtConcurrentOperationsLimitExceeded(YtResponseError):
    """Concurrent operations limit exceeded.
       It is used in retries."""
    pass


class YtMasterDisconnectedError(YtResponseError):
    """Indicates that master has disconnected from scheduler.
       It is used in retries."""
    pass


class YtRequestTimedOut(YtResponseError):
    """Request timed out.
       It is used in retries."""
    pass


class YtNoSuchTransaction(YtResponseError):
    """No such transaction.
       It is used in retries."""
    pass


class YtAccountLimitExceeded(YtResponseError):
    """Account limit exceeded, used to avoid excessive retries."""
    pass


class YtMasterCommunicationError(YtResponseError):
    """Master communication error.
       It is used in retries."""
    pass


class YtChunkUnavailable(YtResponseError):
    """Chunk unavailable error
       It is used in read retries"""
    pass


class YtCypressTransactionLockConflict(YtResponseError):
    """Concurrent transaction lock conflict error.
       It is used in upload_file_to_cache retries."""
    pass


class YtTabletTransactionLockConflict(YtResponseError):
    """Tablet transaction lock conflict error."""
    pass


# Deprecated.
YtConcurrentTransactionLockConflict = YtCypressTransactionLockConflict


class YtNoSuchService(YtResponseError):
    """No such service error"""
    pass


class YtTabletIsInIntermediateState(YtResponseError):
    """Tablet is in intermediate state error"""
    pass


class YtNoSuchTablet(YtResponseError):
    """No such tablet error"""
    pass


class YtTabletNotMounted(YtResponseError):
    """Tablet is not mounted error"""
    pass


class YtNoSuchCell(YtResponseError):
    """No such cell error"""
    pass


class YtTransportError(YtResponseError):
    """Transport error"""
    pass


class YtProxyUnavailable(YtError):
    """Proxy is under heavy load."""
    def __init__(self, response):
        self.response = response
        attributes = {
            "url": response.url,
            "request_info": hide_auth_headers_in_request_info(response.request_info)
        }
        super(YtProxyUnavailable, self).__init__(
            message="Proxy is unavailable",
            attributes=attributes,
            inner_errors=[response.error()])


class YtIncorrectResponse(YtError):
    """Incorrect proxy response."""
    def __init__(self, message, response):
        self.response = response
        attributes = {
            "url": response.url,
            "headers": hide_auth_headers(response.headers),
            "request_info": hide_auth_headers_in_request_info(response.request_info),
            "body": self.truncate(response.text)}
        super(YtIncorrectResponse, self).__init__(message, attributes=attributes)

    def truncate(self, str):
        if len(str) > 100:
            return str[:100] + "...truncated"
        return str


class YtTokenError(YtError):
    """Some problem occurred with authentication token."""
    pass


class YtRetriableError(Exception):
    """Just simple retriable error for test purposes."""
    pass


class YtTransactionPingError(BaseException):
    """Raised in signal handler when thread was unable to ping transaction."""
    pass


class YtAllTargetNodesFailed(YtResponseError):
    """Failed to write chunk since all target nodes have failed."""
    pass


class YtRetriableArchiveError(YtResponseError):
    """Operation progress in Cypress is outdated while archive request failed."""
    pass


class YtResolveError(YtResponseError):
    """Cypress node not found"""
    pass


class YtRowIsBlocked(YtResponseError):
    """Row is blocked"""
    pass


class YtBlockedRowWaitTimeout(YtResponseError):
    """Timed out waiting on blocked row"""
    pass


class YtChunkNotPreloaded(YtResponseError):
    """Chunk data is not preloaded yet"""
    pass


class YtNoInSyncReplicas(YtResponseError):
    """No in-sync replicas found"""
    pass
