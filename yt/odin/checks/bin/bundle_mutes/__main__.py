from yt_odin_checks.lib.bundle_mutes import run_check_impl
from yt_odin_checks.lib.check_runner import main

from typing import Any, Mapping, Tuple


def run_check(
    yt_client: Any,
    logger: Any,
    options: Mapping[str, Any],
    states: Any,
) -> Tuple[float, str]:
    """Run the bundle mutes check."""

    return run_check_impl(yt_client, logger, options, states)


if __name__ == "__main__":
    main(run_check)
