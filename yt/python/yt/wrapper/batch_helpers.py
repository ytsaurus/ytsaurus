def create_batch_client(raise_errors=False, max_batch_size=None, client=None):
    """Creates client which supports batch executions."""
    # TODO: fix in YT-6615
    from .batch_execution import BatchExecutor
    batch_executor = BatchExecutor(raise_errors, max_batch_size, client)
    return batch_executor.get_client()

def batch_apply(function, data, raise_errors=True, client=None):
    """Applies function to each element from data in a batch mode and returns result."""
    # TODO: fix in YT-6615
    from .batch_execution import YtBatchRequestFailedError

    batch_client = create_batch_client(raise_errors=raise_errors, client=client)
    results = []
    for item in data:
        results.append(function(item, client=batch_client))
    batch_client.commit_batch()
    unpacked_results = []
    errors = []
    for result in results:
        if result is None:
            unpacked_results.append(None)
        elif result.is_ok():
            unpacked_results.append(result.get_result())
        else:
            errors.append(result.get_error())
    if errors:
        raise YtBatchRequestFailedError("Batch request failed", inner_errors=errors)
    return unpacked_results
