# Debugging jobs locally

To make job debugging more convenient (for example, with GDB), you can use a special utility, `yt job-tool`. It downloads the job environment along with its input data and generates a script to run it.

The utility comes as part of the [Python API](../../../api/python/start.md) package. All types of jobs that run custom code are supported.

The binary file supports two commands: `prepare-job-environment` and `run-job`. To get detailed information, use the `yt job-tool --help` command.

## Job's input data

By default, `job-tool` gets full job input, although it isn't available for all jobs:

* The specification required to get full input is saved for a few failed and a few successfully completed jobs. It is also available for all running jobs.
* Getting full input requires the data read by the job: input tables must be available and unmodified.
* "reduce_combiner" and "reduce" jobs of a MapReduce operation read temporary data that is only available until the operation has completed. To debug such jobs, you can rewrite the operation as a combination of Map, Sort, and Reduce. Or you can debug them while the operation is still running (including while it is suspended).

## Example

1. Let's run this operation:

    ```python
    import yt.wrapper as yt

    def mapper(rec):
        raise RuntimeError("fail")

    if __name__ == "__main__":
        yt.run_map(mapper, "//home/user/tables/dsv", "//home/user/output", format="dsv")
    ```

2. When the operation fails, we diagnose it:

    ```bash
    yt job-tool prepare-job-environment ffc8f462-7f68f8c3-3fe03e8-433fe11f ffe4ff5c-d2fac8c1-3fe0384-816a7fd0
    ```

3. Once the command has finished, the folder `job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0` will contain the following files:

    ```bash
    $ ls job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0
    command  input  run_gdb.sh  run.sh  sandbox
    ```

    Where:

    * The `command` file contains the command that runs the job. Since the operation was run using the [Python API](../../../api/python/start.md), the code will look like this:

      ```bash
      cat job_ffe4ff5c-d2fac8c1-3fe0384-816a7fd0/command
      python _py_runner.py mapper.lV4l9c config_dump6RRYa4 _modulesEFMotR _main_moduleJALUsC.py _main_module PY_SOURCE%
      ```

    * `input` is the job input; it could be worth analyzing in some cases.
    * `run.sh` is a shell script that runs the job locally.
    * `run_gdb.sh` is a shell script that runs the job with GDB, which is useful for debugging C++ programs.

      {% note info %}

      You can modify the `run_gdb.sh` and `run.sh` scripts, for example, to integrate a custom debugger.

      {% endnote %}

    * `sandbox` is the directory where the job was run, along with all the files necessary to run it.
      As you debug, you can try updating files in the directory.


4. Run the job locally using the `run.sh` script:

    ```bash
    ./run
    2016-07-22 12:00:33,499 INFO    Started job process
    User job exited with non-zero exit code 1 with stderr:
    Traceback (most recent call last):
      File "_py_runner.py", line 56, in <module>
        main()
      File "_py_runner.py", line 53, in main
        yt.wrapper.py_runner_helpers.process_rows(__operation_dump_filename, __config_dump_filename, start_time=start_time)
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 154, in process_rows
        output_format.dump_rows(result, streams.get_original_stdout(), raw=raw)
      File "/home/user/yt/python/yt/wrapper/format.py", line 137, in dump_rows
        self._dump_rows(rows, stream)
      File "/home/user/yt/python/yt/wrapper/format.py", line 251, in _dump_rows
        for row in rows:
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 89, in process_frozen_dict
        for row in rows:
      File "/home/user/yt/python/yt/wrapper/py_runner_helpers.py", line 49, in generator
        result = func(*args)
      File "<stdin>", line 2, in mapper
    RuntimeError: fail

        origin          hostname in 2016-07-22T12:00:35.427161Z
    ```




