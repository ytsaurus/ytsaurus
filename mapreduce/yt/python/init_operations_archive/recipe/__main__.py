import tempfile

from library.python.testing.recipe import declare_recipe

from mapreduce.yt.python.recipe.lib import start, stop

from mapreduce.yt.python.yt_stuff import YtConfig

import yatest.common

def start_and_init(args):
    scheduler_config = tempfile.NamedTemporaryFile(delete=False)
    scheduler_config.write("""
        {
            "scheduler" = {
                "operations_cleaner"= {
                    "enable"= %true;
                    "analysis_period"= 100;
                };
                "enable_job_reporter" = %true;
                "enable_job_stderr_reporter" = %true;
                "enable_job_fail_context_reporter" = %true;
                "enable_job_spec_reporter" = %true;
            };
        }
    """)
    scheduler_config.close()

    node_config = tempfile.NamedTemporaryFile(delete=False)
    node_config.write("""
        {
            "exec_agent"= {
                "statistics_reporter"= {
                    "enabled" = %true;
                    "reporting_period" = 10;
                    "min_repeat_delay" = 10;
                    "max_repeat_delay" = 10;
                };
            };
            "tablet_node" = {
                "resource_limits" = {
                    "slots" = 2;
                };
            };
        }
    """)
    node_config.close()

    yt = start(args, YtConfig(scheduler_config=scheduler_config.name, node_config=node_config.name))
    yt_address = "localhost:" + str(yt.yt_proxy_port)

    yatest.common.execute([
        yatest.common.binary_path("mapreduce/yt/python/init_operations_archive/init_operations_archive"),
        yt_address,
    ])


if __name__ == "__main__":
    declare_recipe(start_and_init, stop)
