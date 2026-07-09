"""Python-side runner for Flow: launches the pipeline in a YT vanilla operation.

Run as ``./my_pipeline --config pipeline.yson --flow-bin <path/to/flow_server>``. The runner
enriches the pipeline config so the worker ships *this* python binary as the companion (plus any
extra files it needs), writes the extended config, then execs the given flow_server
(``flow_server --config <extended>``). flow_server then performs the whole launch: bootstrap the
files into YT, start the operation that runs flow_server (which in turn spawns the companions),
and set the pipeline spec. All launch logic thus lives once, in C++ (``library/cpp/runner``);
Python only adds the companion-specific enrichment.

The flow_server binary is passed explicitly via ``--flow-bin`` rather than embedded, so the
pipeline binary stays light and the flow_server version is chosen by the caller.

Companion-mode invocations (``YT_FLOW_COMPANION_CONFIG`` in env) skip the runner entirely.
"""

import argparse
import logging
import os
import sys
import tempfile

from yt.wrapper import yson

log = logging.getLogger(__name__)

_PYTHON_COMPANION_NAME = "py_companion"
_COMPANION_MANAGER_CLASS = "NYT::NFlow::NCompanion::TCompanionManager"


def parse_launch_args(argv):
    """Return ``(config_path, flow_bin)`` from ``--config`` / ``--flow-bin`` (either may be None)."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=None)
    parser.add_argument("--flow-bin", dest="flow_bin", default=None)
    args, _ = parser.parse_known_args(argv[1:])
    return args.config, args.flow_bin


def launch(config_path, flow_bin):
    """Enrich the pipeline config to ship this binary as the companion, then exec flow_bin."""
    if not config_path:
        raise RuntimeError("--config <pipeline.yson> is required to launch the pipeline")
    if not flow_bin:
        raise RuntimeError("--flow-bin <path to flow_server> is required to launch the pipeline")

    with open(config_path, "rb") as f:
        pipeline_config = yson.load(f)

    vanilla = pipeline_config.get("vanilla")
    if vanilla and vanilla.get("enable"):
        _patch_companion_resources(pipeline_config.setdefault("spec", {}))
        worker = vanilla.setdefault("worker", {})
        # Hand the worker this very binary; flow_server ships it into the job sandbox.
        worker.setdefault("local_files", {})[_PYTHON_COMPANION_NAME] = os.path.abspath(sys.argv[0])

    extended_config = _write_temp_yson(pipeline_config, "extended-pipeline.yson")
    flow_bin = os.path.abspath(flow_bin)
    log.info("Launching %s with extended config %s", flow_bin, extended_config)
    os.execv(flow_bin, [flow_bin, "--config", extended_config])


def _patch_companion_resources(spec):
    """Point every TCompanionManager resource at the shipped python companion binary."""
    for resource_id, resource_def in spec.get("resources", {}).items():
        if resource_def.get("resource_class_name") != _COMPANION_MANAGER_CLASS:
            continue
        parameters = resource_def.setdefault("parameters", {})
        parameters["entrypoint"] = {"executable": f"./{_PYTHON_COMPANION_NAME}"}
        parameters["run_process"] = True
        log.info("Patched companion resource %s to spawn locally", resource_id)


def _write_temp_yson(content, name):
    tmp_dir = tempfile.mkdtemp(prefix="flow_runner_")
    path = os.path.join(tmp_dir, name)
    with open(path, "wb") as f:
        yson.dump(content, f, yson_format="pretty")
    return path
