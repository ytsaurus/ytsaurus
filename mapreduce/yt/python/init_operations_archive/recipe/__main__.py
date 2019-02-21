from library.python.testing.recipe import declare_recipe

from mapreduce.yt.python.recipe.lib import start, stop

from mapreduce.yt.python.yt_stuff import YtConfig

import yatest.common

def start_and_init(args):
    scheduler_config = {
        "scheduler": {
            "operations_cleaner": {
                "enable": True,
                "analysis_period": 100,
            },
            "enable_job_reporter": True,
            "enable_job_stderr_reporter": True,
            "enable_job_fail_context_reporter": True,
            "enable_job_spec_reporter": True,
        },
    }

    node_config = {
        "exec_agent": {
            "statistics_reporter": {
                "enabled": True,
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        },
        "tablet_node": {
            "resource_limits": {
                "slots": 2,
            },
        },
    }

    yt = start(args, YtConfig(scheduler_config=scheduler_config, node_config=node_config))
    yt_address = "localhost:" + str(yt.yt_proxy_port)

    yatest.common.execute([
        yatest.common.binary_path("mapreduce/yt/python/init_operations_archive/init_operations_archive"),
        yt_address,
    ])


if __name__ == "__main__":
    declare_recipe(start_and_init, stop)
