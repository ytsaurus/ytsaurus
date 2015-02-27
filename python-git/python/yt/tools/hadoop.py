import os
import json
from subprocess import check_output

class Hive(object):
    def __init__(self, hcatalog_host, hdfs_host, hive_exporter_library, java_path=""):
        self.hcatalog_host = hcatalog_host
        self.hdfs_host = hdfs_host
        self.hive_exporter_library = hive_exporter_library
        self.java_path = java_path

    def get_table_config_and_files(self, database, table):
        hive_cmd = "exec=set hive.ddl.output.format=json; use {0}; desc extended {1};".format(database, table)
        templeton_url = 'http://{0}/templeton/v1/ddl?user.name=none'.format(self.hcatalog_host)
        hive_response = json.loads(check_output(["curl", "--silent", "--show-error", "-d", hive_cmd, "-X", "POST", templeton_url]))

        config = hive_response["stdout"].strip()
        location = json.loads(config)["tableInfo"]["sd"]["location"]
        relative_path = location.lstrip("hdfs://").split("/", 1)[1]

        webhdfs_url = "http://{0}/webhdfs/v1/{1}?op=LISTSTATUS".format(self.hdfs_host, relative_path)
        list_response = json.loads(check_output(["curl", "--silent", "--show-error", webhdfs_url]))

        return config, [os.path.join(relative_path, filename["pathSuffix"]) for filename in list_response["FileStatuses"]["FileStatus"]]

    def get_read_command(self, read_config):
        return """
set -uxe

while true; do
    set +e
    read -r table;
    result="$?"
    set -e
    if [ "$result" != "0" ]; then break; fi;

    {jar} xf ./{hive_exporter_library} libhadoop.so libsnappy.so.1;
    curl --silent --show-error "http://{hdfs_host}/webhdfs/v1/${{table}}?op=OPEN" >output;
    {java} -Dhadoop.root.logger=INFO -Djava.library.path=./ -jar ./{hive_exporter_library} -file output -config {read_config};
done
"""\
            .format(java=os.path.join(self.java_path, "java"),
                    jar=os.path.join(self.java_path, "jar"),
                    hive_exporter_library=os.path.basename(self.hive_exporter_library),
                    hdfs_host=self.hdfs_host,
                    read_config=read_config)
