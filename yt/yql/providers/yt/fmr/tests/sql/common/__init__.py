import importlib.util
import inspect
import os
import tempfile

from yt import yson
import cyson
from yql_utils import get_table_clusters, replace_vals


def _run_input_generator(generator_py: str, out_dir: str):
    spec = importlib.util.spec_from_file_location("fmr_generated_inputs", generator_py)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load generator module from {generator_py}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "run"):
        raise RuntimeError(f"Generator {generator_py} has no run() function")

    run = module.run
    sig = inspect.signature(run)
    if len(sig.parameters) == 0:
        return run()
    return run(out_dir)


def expand_generated_inputs(config, suite: str, tmpdir, data_path: str):
    """
    Expand cfg entries of form:
      in <prefix> generated <python_file>
    """
    suite_dir = os.path.join(data_path, suite)
    expanded = []

    for item in config:
        if item[0] == "in" and item[2] == "generated":
            prefix = item[1]
            generator_rel = item[3]
            generator_py = generator_rel if os.path.isabs(generator_rel) else os.path.join(suite_dir, generator_rel)
            if not os.path.exists(generator_py):
                raise RuntimeError(f"Input generator not found: {generator_py}")

            out_dir = tempfile.mkdtemp(prefix=f"fmr_generated_{prefix}_", dir=str(tmpdir))
            generated_files = _run_input_generator(generator_py, out_dir)
            if not isinstance(generated_files, (list, tuple)):
                raise RuntimeError(f"Generator {generator_py} must return list/tuple of filenames, got {type(generated_files)}")

            for i, f in enumerate(generated_files, start=1):
                if not isinstance(f, str):
                    raise RuntimeError(f"Generator {generator_py} must return list of strings, got element {type(f)}")
                file_name = f.split("/")[-1].split(".")[0]
                path = f if os.path.isabs(f) else os.path.join(out_dir, f)
                expanded.append(["in", file_name, path])
            continue

        expanded.append(item)

    return expanded


def sort_yson(yson):
    for res in yson:
        for data in res[b'Write']:
            if b'Unordered' in res and b'Data' in data:
                data[b'Data'] = sorted(data[b'Data'])
    return yson


def add_table_clusters(suite, config, data_path):
    clusters = get_table_clusters(suite, config, data_path)
    if not clusters:
        return None

    def patch(cfg_message):
        for c in sorted(clusters):
            mapping = cfg_message.Yt.ClusterMapping.add()
            mapping.Name = c
    return patch


def get_yson_from_file_sql_query_result(query_result):
    res_yson = query_result.results
    res_yson = cyson.loads(res_yson) if res_yson else cyson.loads("[]")
    res_yson = replace_vals(res_yson)
    return sort_yson(res_yson)


def get_yson_from_yt_sql_query_result(query_result):
    yt_res_yson = query_result.results.get('data', [])
    return replace_vals(cyson.loads(yson.dumps(yt_res_yson)))
