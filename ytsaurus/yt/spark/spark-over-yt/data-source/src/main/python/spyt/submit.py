import os
import shutil
import signal
import tempfile
import time
from contextlib import contextmanager
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark.serializers import read_int, UTF8Deserializer
from subprocess import Popen, PIPE
from pyspark.find_spark_home import _find_spark_home
from py4j.protocol import Py4JJavaError
from enum import Enum
from datetime import timedelta
from .utils import scala_buffer_to_list


def launch_gateway(memory="512m",
                   java_home=None,
                   java_opts=None,
                   additional_jars=None,
                   additional_environ=None):
    spark_home = _find_spark_home()
    java = os.path.join(java_home, "bin", "java") if java_home else "java"
    additional_jars = additional_jars or []

    command = [java, "-Xmx{}".format(memory)]
    command += java_opts or []
    command += ["-cp", ":".join(additional_jars + _submit_classpath()), "tech.ytsaurus.spyt.submit.PythonGatewayServer"]

    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env.update(additional_environ or {})
        env["_SPYT_SUBMIT_CONN_INFO_PATH"] = conn_info_file
        env["SPARK_HOME"] = spark_home

        # Launch the Java gateway.
        popen_kwargs = {'stdin': PIPE, 'env': env}

        # Don't send ctrl-c / SIGINT to the Java gateway:
        def preexec_func():
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        popen_kwargs['preexec_fn'] = preexec_func
        proc = Popen(command, **popen_kwargs)

        # Wait for the file to appear, or for the process to exit, whichever happens first.
        while not proc.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            raise Exception("Java gateway process exited before sending its port number")

        with open(conn_info_file, "rb") as info:
            gateway_port = read_int(info)
            gateway_secret = UTF8Deserializer().loads(info)
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway (or client server to pin the thread between JVM and Python)
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port,
                                             auth_token=gateway_secret,
                                             auto_convert=True))

    # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
    gateway.proc = proc
    return gateway

def _spyt_path():
    import spyt
    return spyt.__path__[0]

def _list_path(path):
    return [os.path.join(path, name) for name in os.listdir(path)]

def _submit_classpath():
    spark_home = _find_spark_home()
    spark_classpath = _list_path(os.path.join(spark_home, "jars"))
    spyt_classpath = _list_path(os.path.join(_spyt_path(), "jars"))
    return spyt_classpath + spark_classpath


def shutdown_gateway(gateway):
    gateway.shutdown()
    gateway.proc.stdin.close()


@contextmanager
def java_gateway(*args, **kwargs):
    gateway = launch_gateway(*args, **kwargs)
    try:
        yield gateway
    except Py4JJavaError as e:
        raise RuntimeError(str(e))
    finally:
        shutdown_gateway(gateway)


class RetryConfig(object):
    def __new__(cls, *args, **kwargs):
        instance = super(RetryConfig, cls).__new__(cls)
        return instance

    def __init__(self, enable_retry=True, retry_limit=10, retry_interval=timedelta(minutes=1)):
        self.enable_retry = enable_retry
        self.retry_limit = retry_limit
        self.retry_interval = retry_interval

    def _to_java(self, gateway):
        jduration = gateway.jvm.tech.ytsaurus.spyt.submit.RetryConfig.durationFromSeconds(
            int(self.retry_interval.total_seconds()))
        return gateway.jvm.tech.ytsaurus.spyt.submit.RetryConfig(self.enable_retry,
                                                                 self.retry_limit,
                                                                 jduration)


class SparkSubmissionClient(object):
    def __new__(cls, *args, **kwargs):
        instance = super(SparkSubmissionClient, cls).__new__(cls)
        return instance

    def __init__(self, gateway, proxy, discovery_path, spyt_version, user, token):
        self._jclient = gateway.jvm.tech.ytsaurus.spyt.submit.SubmissionClient(proxy, discovery_path,
                                                                               spyt_version, user, token)
        self.gateway = gateway

    def new_launcher(self):
        jlauncher = self._jclient.newLauncher()
        return SparkLauncher(jlauncher, self.gateway)

    def submit(self, launcher, retry_config=None):
        if retry_config is None:
            retry_config = RetryConfig()
        jresult = self._jclient.submit(launcher._jlauncher, retry_config._to_java(self.gateway))
        if jresult.isFailure():
            raise RuntimeError(jresult.failed().get().getMessage())
        return jresult.get()

    def kill(self, submission_id):
        return self._jclient.kill(submission_id)

    def get_active_drivers(self):
        return scala_buffer_to_list(self._jclient.getActiveDrivers())

    def get_completed_drivers(self):
        return scala_buffer_to_list(self._jclient.getCompletedDrivers())

    def get_all_drivers(self):
        return scala_buffer_to_list(self._jclient.getAllDrivers())

    def get_status(self, submission_id):
        return SubmissionStatus.from_string(self._jclient.getStringStatus(submission_id))

    def get_app_status(self, driver_id):
        return ApplicationStatus.from_string(self._jclient.getStringApplicationStatus(driver_id))


class SubmissionStatus(Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    RELAUNCHING = "RELAUNCHING"
    UNKNOWN = "UNKNOWN"
    KILLED = "KILLED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    UNDEFINED = "UNDEFINED"

    @staticmethod
    def from_string(name):
        return next(member for n, member in SubmissionStatus.__members__.items() if n == name)

    @staticmethod
    def is_final(status):
        return (
                status is SubmissionStatus.FINISHED or
                status is SubmissionStatus.UNKNOWN or
                status is SubmissionStatus.KILLED or
                status is SubmissionStatus.FAILED or
                status is SubmissionStatus.ERROR
        )

    @staticmethod
    def is_success(status):
        return (
                status is SubmissionStatus.FINISHED
        )

    @staticmethod
    def is_failure(status):
        return (
                status is SubmissionStatus.UNKNOWN or
                status is SubmissionStatus.KILLED or
                status is SubmissionStatus.FAILED or
                status is SubmissionStatus.ERROR
        )


class ApplicationStatus(Enum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"
    UNDEFINED = "UNDEFINED"

    @staticmethod
    def from_string(name):
        return next(member for n, member in ApplicationStatus.__members__.items() if n == name)

    @staticmethod
    def is_final(status):
        return (
                status is ApplicationStatus.FINISHED or
                status is ApplicationStatus.UNKNOWN or
                status is ApplicationStatus.KILLED or
                status is ApplicationStatus.FAILED
        )

    @staticmethod
    def is_success(status):
        return (
                status is ApplicationStatus.FINISHED
        )

    @staticmethod
    def is_failure(status):
        return (
                status is ApplicationStatus.UNKNOWN or
                status is ApplicationStatus.KILLED or
                status is ApplicationStatus.FAILED
        )

    @staticmethod
    def is_waiting(status):
        return (
                status is ApplicationStatus.WAITING
        )


# wraps methods from
# https://github.yandex-team.ru/sashbel/spark/blob/3.0.1-yt/launcher/src/main/java/org/apache/spark/launcher/AbstractLauncher.java
class SparkLauncher(object):
    def __new__(cls, *args, **kwargs):
        instance = super(SparkLauncher, cls).__new__(cls)
        return instance

    def __init__(self, jlauncher, gateway):
        self._jlauncher = jlauncher
        self._jutils = gateway.jvm.tech.ytsaurus.spyt.submit.InProcessLauncherPythonUtils

    def set_app_resource(self, resource):
        self._jlauncher.setAppResource(resource)
        return self

    def set_main_class(self, main_class):
        self._jlauncher.setMainClass(main_class)
        return self

    def set_conf(self, key, value):
        self._jlauncher.setConf(key, value)
        return self

    def set_properties_file(self, path):
        self._jlauncher.setPropertiesFile(path)
        return self

    def set_app_name(self, app_name):
        self._jlauncher.setAppName(app_name)
        return self

    def add_spark_arg(self, arg, value=None):
        if value:
            self._jlauncher.addSparkArg(arg, value)
        else:
            self._jlauncher.addSparkArg(arg)
        return self

    def add_app_args(self, *args):
        for arg in args:
            self._jutils.addAppArg(self._jlauncher, arg)
        return self

    def add_jar(self, jar):
        self._jlauncher.addJar(jar)
        return self

    def add_file(self, file):
        self._jlauncher.addFile(file)
        return self

    def add_py_file(self, file):
        self._jlauncher.addPyFile(file)
        return self

    def set_verbose(self, verbose):
        self._jlauncher.setVerbose(verbose)
