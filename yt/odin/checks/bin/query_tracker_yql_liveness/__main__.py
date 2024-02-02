from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.query_tracker_engine_liveness import Data, run_check_impl


VALUES_COUNT = 10

SCHEMA = [{"name": "x", "type": "int64"}]
SOURCE_DATA = [{"x": i} for i in range(VALUES_COUNT)]
RESULT_DATA = [{"result": i + 1} for i in range(VALUES_COUNT)]


def run_check(yt_client, logger, options, states):
    stage = options["cluster_name_to_query_tracker_stage"].get(options["cluster_name"], "production")

    return run_check_impl(
        yt_client,
        yt_client,
        logger,
        stage,
        states,
        "yql",
        "select x + 1 as result from `{table}`",
        Data(SCHEMA, SOURCE_DATA, RESULT_DATA),
    )


if __name__ == "__main__":
    main(run_check)
