from importlib import import_module
import logging
import os
import yatest


logger = logging.getLogger(__name__)


def quick_setup(yt_client, python_path, java_home):
    if yt_client.exists("//home/spark"):
        logger.info("SPYT root directory already exists")
        return

    publish_artifacts(yt_client)
    set_python_path(python_path, yt_client)
    set_java_home(java_home, yt_client)
    set_executor_conf(yt_client)


def publish_artifacts(yt_client):
    build_path = yatest.common.build_path("yt/spark/spark-over-yt/e2e-test/src/test/yt/data")
    sources_path = os.path.join(build_path, "spyt_cluster")
    print(f"All resources in build directory {build_path}")

    # Dash usage workaround
    module_path = "yt.spark.spark-over-yt.tools.release.publisher"
    config_generator = import_module(module_path + ".config_generator")
    local_manager = import_module(module_path + ".local_manager")
    publish_cluster = import_module(module_path + ".publish_cluster")
    remote_manager = import_module(module_path + ".remote_manager")

    client_builder = remote_manager.ClientBuilder(yt_client=yt_client)
    default_version = local_manager.PackedVersion({"scala": "1.0.0"})
    uploader = remote_manager.Client(client_builder)
    versions = local_manager.Versions(default_version, default_version, default_version)
    config_generator.make_configs(sources_path, client_builder, versions, os_release=True)
    print("Config files generated")

    publish_conf = remote_manager.PublishConfig()
    publish_cluster.upload_spark_fork(uploader, versions, sources_path, publish_conf)
    publish_cluster.upload_cluster(uploader, versions, sources_path, publish_conf)
    publish_cluster.upload_client(uploader, versions, sources_path, publish_conf)
    print("Cluster files uploaded")


def set_spark_conf(options, yt_client):
    spark_conf = yt_client.get("//home/spark/conf/global/spark_conf")
    spark_conf.update(options)
    yt_client.set("//home/spark/conf/global/spark_conf", spark_conf)


def set_python_path(python_path, yt_client):
    print(f"Set python interpreter: {python_path}")
    python_cluster_paths = {"3.11": python_path}
    yt_client.set("//home/spark/conf/global/python_cluster_paths", python_cluster_paths)
    set_spark_conf({'spark.pyspark.python': python_path}, yt_client)


def set_java_home(java_home, yt_client):
    print(f"Set java home: {java_home}")
    yt_client.set("//home/spark/conf/global/environment/JAVA_HOME", java_home)


def set_executor_conf(yt_client, executor_cores=1, executor_memory='1g'):
    set_spark_conf({'spark.executor.cores': str(executor_cores), 'spark.executor.memory': executor_memory}, yt_client)
