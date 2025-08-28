import functools
from shutil import copyfileobj
import typing as tp  # noqa: F401
import io  # noqa: F401

from exts.decompress import udopen
from devtools.ya.yalibrary import sjson


FILE_PROTO = 'file://'


def dump_graph_as_json(graph, fp):
    # type: (tp.Any, io.BytesIO) -> None
    if isinstance(graph, str) and graph.startswith(FILE_PROTO):
        with udopen(graph[len(FILE_PROTO) :], 'rb') as src_fp:
            copyfileobj(src_fp, fp)
    else:
        sjson.dump(graph, fp)


def dump_context_as_json(context, fp):
    # type: (tp.Any, io.BytesIO) -> None
    context_to_dump = context
    if isinstance(context, dict):
        # Expected context has a small amount of keys so copy is cheap
        context_to_dump = context.copy()
        if 'graph' in context:
            context_to_dump['graph'] = functools.partial(dump_graph_as_json, context['graph'])
    sjson.dump(context_to_dump, fp)
