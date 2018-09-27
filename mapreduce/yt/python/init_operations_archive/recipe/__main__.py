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
    yt = start(args, YtConfig(scheduler_config=scheduler_config.name))
    yt_address = "localhost:" + str(yt.yt_proxy_port)

#    XXX
#    yatest.common.execute([
#        yatest.common.binary_path("mapreduce/yt/python/init_operations_archive/init_operations_archive"),
#        yt_address
#    ])


if __name__ == "__main__":
    declare_recipe(start_and_init, stop)
