import argparse
import logging
import sys

import yt.wrapper as yt
from yt.wrapper.errors import YtResponseError

EPILOG = """Example:

Download JFR from a companion on the given worker:
  {prog} --pipeline-path zeno://home/pipelines/pipeline \\
      --worker "[2a02:6b8:c24:14a3:0:615d:570f:1]:81" \\
      --output recording.jfr
""".format(prog=sys.argv[0])

logger = logging.getLogger("download_jfr")


def setup_logging(verbose):
    logger.handlers.clear()
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)


def parse_pipeline_path(raw_path):
    """Parse pipeline path in 'cluster://path' or '//path' format.

    Returns:
        (cluster_or_none, path)
    """
    if "://" in raw_path and not raw_path.startswith("//"):
        cluster, path = raw_path.split("://", 1)
        if not path.startswith("//"):
            path = "//" + path.lstrip("/")
        return cluster, path

    return None, raw_path


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download JFR recording from a Flow pipeline Java companion.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=EPILOG,
    )

    parser.add_argument("--proxy", type=str, required=False, default=None, help="YT proxy")
    parser.add_argument("--pipeline-path", type=str, required=True, help="Path to the flow pipeline")

    worker_group = parser.add_mutually_exclusive_group(required=True)
    worker_group.add_argument("--worker", type=str, help="Worker address (e.g. '[::1]:81')")

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help="Output file path. Defaults to 'recording_<worker>.jfr'",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug output")

    args = parser.parse_args()

    cluster, args.pipeline_path = parse_pipeline_path(args.pipeline_path)
    if cluster and args.proxy is None:
        args.proxy = cluster

    if args.proxy is None:
        parser.error("--proxy is required (or use 'cluster://path' format for --pipeline-path)")

    return args


def sanitize_worker_for_filename(worker_address):
    """Convert worker address like '[2a02:6b8:c24:14a3:0:615d:570f:1]:81' to a safe filename part."""
    return worker_address.replace("[", "").replace("]", "").replace(":", "_").replace("/", "_")


def download_jfr_from_worker(yt_client, pipeline_path, worker_address):
    """Download JFR recording from a companion on the given worker.

    The get-worker-orchid command returns {"value": <orchid_data>}.
    The orchid node at /companion/jfr returns a dict:
      - {"data": "<JFR bytes>"} on success
      - {"error": "<reason>"}   when JFR is not available

    Returns:
        bytes: JFR binary data on success.

    Raises:
        RuntimeError: If JFR is not available or response format is unexpected.
        YtResponseError: If the flow_execute call fails.
    """
    logger.info("Requesting JFR from companion at %s ...", worker_address)

    result = yt_client.flow_execute(
        pipeline_path,
        "get-worker-orchid",
        {"worker": worker_address, "path": "/companion/jfr"},
    )

    # get-worker-orchid wraps the orchid value in {"value": ...}.
    value = result["value"]

    if not isinstance(value, dict):
        raise RuntimeError(
            "Unexpected response type: expected dict, got {} (repr: {})".format(type(value).__name__, repr(value)[:200])
        )

    if "error" in value:
        raise RuntimeError(value["error"])

    if "data" not in value:
        raise RuntimeError(
            "Unexpected response: dict has neither 'data' nor 'error' key (keys: {})".format(list(value.keys()))
        )

    data = value["data"]

    if isinstance(data, bytes):
        return data

    # Handle YsonStringProxy — binary YSON string wrapper.
    if hasattr(data, "_bytes"):
        return data._bytes

    raise RuntimeError("Unexpected data type in response: {} (repr: {})".format(type(data).__name__, repr(data)[:200]))


def main():
    args = parse_args()
    setup_logging(args.verbose)

    yt_client = yt.YtClient(proxy=args.proxy)

    output_path = args.output
    if output_path is None:
        output_path = "recording_{}.jfr".format(sanitize_worker_for_filename(args.worker))

    try:
        data = download_jfr_from_worker(yt_client, args.pipeline_path, args.worker)
    except RuntimeError as e:
        logger.error("JFR download failed: %s", e)
        sys.exit(1)
    except YtResponseError as e:
        logger.error("flow_execute failed: %s", e)
        sys.exit(1)

    with open(output_path, "wb") as f:
        f.write(data)
    logger.info("Saved %d bytes to %s", len(data), output_path)


if __name__ == "__main__":
    main()
