from yt.wrapper import yson

from yt.yt.flow.library.python.client.flow_view.bindings import decompress


def get_flow_view(client, pipeline_path, view_path=None, cache=None):
    """Fetch a YT Flow flow view via the flow_execute commands.

    Drop-in replacement for ``client.get_flow_view``: returns the same parsed YSON structure. If the
    controller advertises ``get-flow-view-v2`` (checked via the ``list`` command) the compressed command is
    used and the result is decompressed with the same codec the controller used (the codec id travels in the
    response); otherwise it falls back to the uncompressed ``get-flow-view``.
    """
    argument = {}
    if view_path is not None:
        argument["path"] = view_path
    if cache is not None:
        argument["cache"] = cache

    commands = client.flow_execute(pipeline_path, "list")
    if any(yson.get_bytes(command) == b"get-flow-view-v2" for command in commands):
        compressed = client.flow_execute(pipeline_path, "get-flow-view-v2", argument)
        return yson.loads(decompress(yson.get_bytes(compressed["codec"]), yson.get_bytes(compressed["data"])))
    return client.flow_execute(pipeline_path, "get-flow-view", argument)
