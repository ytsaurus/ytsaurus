import argparse
import logging
import os
import tempfile

import yt.wrapper
import yt_driver_rpc_bindings
from yt.wrapper import yson

from yt.ypath.rich import RichYPath

from yt.yt.flow.tools.draw_pipeline_graph.dot_graph import get_dot_binary_path, render_dot_graph
from yt.yt.flow.tools.draw_pipeline_graph.graph_model import get_graph
from yt.yt.flow.tools.draw_pipeline_graph.mermaid_graph import render_mermaid_graph
from yt.yt.flow.library.python.client.flow_view import get_flow_view

from yt.yt.flow.tools.draw_pipeline_graph.bindings import (
    flow_view_yson_to_mermaid_computations_graph,
    flow_view_yson_to_mermaid_unrolled_graph,
)


def setup_logging(args):
    py_level = logging.DEBUG if args.verbose else logging.INFO

    root_logger = logging.getLogger()
    while len(root_logger.handlers) > 0:
        root_logger.removeHandler(root_logger.handlers[0])
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s"))
    root_logger.addHandler(handler)
    root_logger.setLevel(py_level)

    logging.getLogger("Yt").setLevel(py_level)

    logging_config = {
        "rules": [
            {
                "min_level": "debug" if args.verbose else "error",
                "writers": ["stderr"],
            },
        ],
        "writers": {"stderr": {"type": "stderr"}},
    }
    yt_driver_rpc_bindings.configure_logging(logging_config)


def parse_args():
    argparser = argparse.ArgumentParser(
        description="Draws pipeline graph. If output is not configured, uploads result to mds with configured ttl"
    )
    argparser.add_argument(
        "--input",
        type=str,
        required=True,
        help="`cluster://path` address of pipeline",
    )
    argparser.add_argument(
        "--orientation", type=str, choices=["vertical", "horizontal"], default="vertical", help="Arrows orientation"
    )
    argparser.add_argument(
        "--check-is-full",
        action="store_true",
        help="If gathered info is incomplete, this fact is logged and exit status is not 0",
    )
    argparser.add_argument("--verbose", action="store_true", help="Print more logs")
    argparser.add_argument(
        "--timeout",
        type=float,
        default=60,
        help="Per-request timeout in seconds for YT calls (get_flow_view etc.). "
        "The default RPC timeout is too low for heavy pipelines.",
    )
    argparser.add_argument("--ttl", type=str, default="14", help="TTL in days for ya upload")
    argparser.add_argument(
        "--output", type=str, default=None, help="File path to save result into. Save nothing if empty"
    )
    argparser.add_argument(
        "--dot-output", type=str, default=None, help="File path to save dot result into. Save nothing if empty"
    )
    argparser.add_argument("--use-embedded-dot", action="store_true", help="Use embedded dot for running in distbuild")
    argparser.add_argument(
        "--mermaid",
        action="store_true",
        default=False,
        help="Render Python mermaid HTML diagram instead of dot SVG. Use --output to set the output file path.",
    )
    argparser.add_argument(
        "--mermaid-cpp",
        action="store_true",
        default=False,
        help="Render C++ mermaid HTML diagram (computations + unrolled graphs). Use --output to set the output file path.",
    )
    args = argparser.parse_args()

    args.pipeline_path, path_attributes = RichYPath().parse(args.input)
    args.cluster_name = path_attributes["cluster"]

    return args


def _render_mermaid_cpp_html(computations_diagram, unrolled_diagram):
    """Wrap two C++ Mermaid diagram strings in a standalone HTML page."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <title>Pipeline graph</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
  <script>mermaid.initialize({{startOnLoad: true, maxTextSize: 1000000}});</script>
  <style>
    body {{ font-family: sans-serif; padding: 1em; }}
    h2 {{ margin-top: 2em; }}
    .diagram-header {{ display: flex; align-items: center; gap: 1em; }}
    .mermaid {{ overflow: auto; border: 1px solid #ccc; padding: 1em; }}
    .copy-btn {{
      font-size: 2em; padding: 0.5em 1.4em; cursor: pointer;
      border: 1px solid #888; border-radius: 4px; background: #f5f5f5;
    }}
    .copy-btn:active {{ background: #ddd; }}
  </style>
</head>
<body>
  <h1>Pipeline graph</h1>

  <div class="diagram-header">
    <h2>Computations graph</h2>
    <button class="copy-btn" onclick="copyDiagram(this, 'comp-src')">Copy mermaid as text</button>
  </div>
  <textarea id="comp-src" style="display:none">{computations_diagram}</textarea>
  <div class="mermaid">
{computations_diagram}
  </div>

  <div class="diagram-header">
    <h2>Unrolled graph</h2>
    <button class="copy-btn" onclick="copyDiagram(this, 'unrolled-src')">Copy mermaid as text</button>
  </div>
  <textarea id="unrolled-src" style="display:none">{unrolled_diagram}</textarea>
  <div class="mermaid">
{unrolled_diagram}
  </div>

  <script>
    function copyDiagram(btn, srcId) {{
      var text = document.getElementById(srcId).value;
      navigator.clipboard.writeText(text).then(function() {{
        var orig = btn.textContent;
        btn.textContent = 'Copied!';
        setTimeout(function() {{ btn.textContent = orig; }}, 1500);
      }}).catch(function(err) {{
        alert('Copy failed: ' + err);
      }});
    }}
  </script>
</body>
</html>
"""


def main():
    args = parse_args()
    setup_logging(args)

    logging.info("Run draw pipeline graph tool with arguments: %s", args)

    if args.mermaid_cpp:
        # C++ mermaid path: fetch flow_view, serialize to YSON, call C++ bindings.
        config = yt.wrapper.default_config.get_config_from_env()
        config["backend"] = "rpc"
        yt_client = yt.wrapper.YtClient(proxy=args.cluster_name, config=config)
        # Raise the per-request timeout; the default RPC timeout is too low for heavy pipelines.
        yt.wrapper.config.set_command_param("timeout", int(args.timeout * 1000), yt_client)
        flow_view = get_flow_view(yt_client, args.pipeline_path, cache=True)
        logging.info("Loaded flow view")

        flow_view_yson = bytes(yson.dumps(flow_view))

        computations_diagram = flow_view_yson_to_mermaid_computations_graph(
            flow_view_yson,
            args.orientation,
            args.cluster_name,
            args.pipeline_path,
        )
        logging.info("Built mermaid computations graph")

        unrolled_diagram = flow_view_yson_to_mermaid_unrolled_graph(
            flow_view_yson,
            args.orientation,
            args.cluster_name,
            args.pipeline_path,
        )
        logging.info("Built mermaid unrolled graph")

        html = _render_mermaid_cpp_html(computations_diagram, unrolled_diagram)
        file_path = args.output or os.path.join(tempfile.mkdtemp(), "pipeline_graph.html")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html)
        logging.info("C++ mermaid diagram saved to %s", file_path)
        if not args.output:
            os.system(f"ya upload --mds {file_path} --ttl {args.ttl}")
        return 0

    # Python path (dot or Python mermaid).
    graph, is_full = get_graph(args)

    if args.mermaid:
        file_path = args.output or os.path.join(tempfile.mkdtemp(), "pipeline_graph.html")
        render_mermaid_graph(graph, file_path, args.orientation)
        logging.info("Mermaid diagram saved to %s", file_path)
        if not args.output:
            os.system(f"ya upload --mds {file_path} --ttl {args.ttl}")
    else:
        dot_binary_path = get_dot_binary_path() if args.use_embedded_dot else None
        file_path = render_dot_graph(graph, args, dot_binary_path)
        if not args.output:
            os.system(f"ya upload --mds {file_path} --ttl {args.ttl}")

    if args.check_is_full and not is_full:
        logging.error("Graph is not full")
        return 1
    return 0


if __name__ == "__main__":
    exit(main())
