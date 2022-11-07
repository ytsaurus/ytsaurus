def create_batch_client(raise_errors=False, max_batch_size=None, concurrency=None, client=None):
    """Creates client which supports batch executions."""
    # TODO: fix in YT-6615
    from .batch_execution import BatchExecutor
    batch_executor = BatchExecutor(raise_errors, max_batch_size, concurrency, client)
    return batch_executor.get_client()


def batch_apply(function, data, client=None):
    """Applies function to each element from data in a batch mode and returns result."""
    batch_client = create_batch_client(raise_errors=True, client=client)
    results = []
    for item in data:
        results.append(function(item, client=batch_client))
    batch_client.commit_batch()
    return [result.get_result() if result is not None else None for result in results]
