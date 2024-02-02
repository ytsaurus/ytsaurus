from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.query_tracker_engine_liveness import Data, run_check_impl
from yt.wrapper import YtClient


VALUES_COUNT = 10

SCHEMA = [{"name": "x", "type": "int64"}]
SOURCE_DATA = [{"x": i} for i in range(VALUES_COUNT)]
RESULT_DATA = [{"result": i + 1} for i in range(VALUES_COUNT)]


def run_check(secrets, yt_client, logger, options, states):
    chyt_stage_client = YtClient(proxy=options["chyt_cluster_name"], token=secrets["yt_token"])
    stage = options["cluster_name_to_query_tracker_stage"].get(options["cluster_name"], "production")

    return run_check_impl(
        yt_client,
        chyt_stage_client,
        logger,
        stage,
        states,
        "chyt",
        "select x + 1 as result from `{table}`",
        Data(SCHEMA, SOURCE_DATA, RESULT_DATA),
        {"clique": "ch_public", "cluster": options["chyt_cluster_name"]}
    )


if __name__ == "__main__":
    main(run_check)
