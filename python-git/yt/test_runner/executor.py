from yt.common import makedirp

import logging
import logging.handlers
import os
import sys


logger = logging.getLogger("Executor")

def get_pytest_item_location_str(item):
    file_path, line_number, function_name = item.location
    return "{} at {}:{}".format(function_name, file_path, line_number)

def _serialize_report(report):
    import py
    d = report.__dict__.copy()
    if hasattr(report.longrepr, "toterminal"):
        d["longrepr"] = str(report.longrepr)
    else:
        d["longrepr"] = report.longrepr

    for name in d:
        if isinstance(d[name], py.path.local):
            d[name] = str(d[name])
        elif name == "result":
            d[name] = None  # for now

    return d

class Executor(object):
    def __init__(self, config, channel):
        config.pluginmanager.register(self)
        logger.info("Executor was registered as pytest plugin")
        self.channel = channel
        self.config = config

    def _send_to_channel(self, data_type, data):
        self.channel.send({"type": data_type, "data": data})

    def pytest_collection_finish(self, session):
        logger.info("Sending discovered test count to master")
        self._send_to_channel("test_count", len(session.items))

    def pytest_runtestloop(self, session):
        test_indices = self.channel.receive()
        logger.info("Received the following test indices: %s", str(test_indices))
        logger.info("Discovered tests count: %d", len(session.items))

        for index, test_index in enumerate(test_indices):
            logger.info("Running test %d", test_index)
            if index != len(test_indices) - 1:
                next_item = session.items[test_indices[index + 1]]
            else:
                next_item = None

            self._send_to_channel("next_test_index", index)

            logger.info("Executing %s", get_pytest_item_location_str(session.items[test_index]))
            self.config.hook.pytest_runtest_protocol(
                item=session.items[test_index],
                nextitem=next_item)

        logger.info("Test loop finished, exiting")

        return True

    def pytest_runtest_call(self, item):
        logger.info("Sending next test stderrs paths to master")
        if not hasattr(item, "cls"):
            return

        result = []
        if hasattr(item.cls, "Env"):
            result.append(item.cls.Env.stderrs_path)

        if hasattr(item.cls, "test_name") and hasattr(item.cls, "run_id") and \
                "TESTS_SANDBOX_STORAGE" in os.environ:
            result.append(os.path.join(
                os.environ["TESTS_SANDBOX_STORAGE"], item.cls.test_name, item.cls.run_id))

        self._send_to_channel("next_test_stderrs_paths", result)

    def pytest_runtest_logreport(self, report):
        logger.info("Sending %s report to master", report.when)

        stderr = None
        for name, value in report.sections:
            if "stderr" in name:
                stderr = value

        if stderr is not None:
            # Location example: ('test_value1.py', 9, 'Test1.test_smth[1]')
            if hasattr(report, "environment_path"):
                path = os.path.join(report.environment_path, "tests_stderrs")
                file_name = report.location[2].rsplit(".", 1)[1].partition("[")[0]
            else:
                path = os.path.join(os.environ.get("TESTS_SANDBOX", ""), "tests_stderrs")
                file_name = report.location[2] \
                    .replace("[", "_") \
                    .replace("]", "_") \
                    .replace(".", "_")

            makedirp(path)

            if sys.version_info[0] < 3:
                stderr = stderr.encode("utf-8")

            with open(os.path.join(path, file_name), "a") as fout:
                fout.write(stderr + "\n")

        self._send_to_channel("report", _serialize_report(report))

def _configure_logger(config):
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        if config.option.debug:
            handler = logging.handlers.WatchedFileHandler("executor_{0}.log".format(os.getpid()))
        else:
            handler = logging.NullHandler()
        logger.addHandler(handler)
    logger.handlers[0].setFormatter(logging.Formatter("%(asctime)-15s\t%(levelname)s\t%(message)s"))

if __name__ == "__channelexec__":
    channel = channel  # noqa
    option_dict, args = channel.receive()
    option_dict["plugins"].append("no:terminal")

    from _pytest.config import Config
    config = Config.fromdictargs(option_dict, args)
    config.args = args
    config.option.usepdb = False
    # Disable parallel testing in subprocess.
    del config.option.process_count

    _configure_logger(config)

    logger.info("Config was received from master. Logging started")

    import_path = os.getcwd()
    sys.path.insert(0, import_path)
    os.environ["PYTHONPATH"] = \
        import_path + os.pathsep + os.environ.get("PYTHONPATH", "")

    executor = Executor(config, channel)
    config.hook.pytest_cmdline_main(config=config)
