import yt.logger as logger
from yt.common import YtError
from yt.wrapper.common import generate_uuid 

import os
import sh
import subprocess32 as subprocess
import simplejson as json
from urllib import quote_plus

class YamrError(YtError):
    pass

_check_output = subprocess.check_output

def _check_call(command, **kwargs):
    logger.debug("Executing command '{}'".format(command))
    timeout = kwargs.pop('timeout', None)
    proc = subprocess.Popen(command, stderr=subprocess.PIPE, **kwargs)
    try:
        _, stderrdata = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise

    if proc.returncode != 0:
        error = YamrError("Command '{0}' failed".format(command))
        error.inner_errors = [YamrError(stderrdata, proc.returncode)]
        raise error

    logger.debug("Command '{}' successfully executed".format(command))

class Yamr(object):
    def __init__(self, binary, server, server_port, http_port, proxies=None, proxy_port=None, fetch_info_from_http=False, mr_user="tmp", fastbone=False, opts=""):
        self.binary = binary
        self.binary_name = os.path.basename(binary)
        self.server = self._make_address(server, server_port)
        self.http_server = self._make_address(server, http_port)
        if proxies is None:
            self.proxies = []
        else:
            self.proxies = map(lambda proxy: self._make_address(proxy, proxy_port), proxies)
        self.fetch_info_from_http = fetch_info_from_http

        self.cache = {}
        self.mr_user = mr_user
        self.opts = opts
        self.fastbone = fastbone

        self._light_command_timeout = 20.0

        # Check that binary exists and supports help
        _check_output("{0} --help".format(self.binary), shell=True)

        self.supports_shared_transactions = \
            subprocess.call("{0} --help | grep sharedtransaction >/dev/null".format(self.binary), shell=True) == 0

        logger.info("Yamr options configured (binary: %s, server: %s, http_server: %s, proxies: [%s])",
                    self.binary, self.server, self.http_server, ", ".join(self.proxies))

    def _make_address(self, address, port):
        if ":" in address and address.rsplit(":", 1)[1].isdigit():
            address = address.rsplit(":", 1)[0]
        return "{0}:{1}".format(address, port)


    def get_field_from_page(self, table, field):
        """ Extract value of given field from http page of the table """
        http_content = sh.curl("{0}/debug?info=table&table={1}".format(self.http_server, table)).stdout
        records_line = filter(lambda line: line.find(field) != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace("<br>", "").replace("<b>", "").replace(",", "")
        return records_line.split(field + ":")[1].split()[0]

    def get_field_from_server(self, table, field, allow_cache):
        if not allow_cache or table not in self.cache:
            output = _check_output(
                "{0} -server {1} -list -prefix {2} -jsonoutput"\
                    .format(self.binary, self.server, table),
                timeout=self._light_command_timeout,
                shell=True)
            table_info = filter(lambda obj: obj["name"] == table, json.loads(output))
            if table_info:
                self.cache[table] = table_info[0]
            else:
                self.cache[table] = None

        if self.cache[table] is None:
            return None

        return self.cache[table].get(field, None)

    def records_count(self, table, allow_cache=False):
        if self.fetch_info_from_http:
            return int(self.get_field_from_page(table, "Records"))
        else:
            return self._as_int(self.get_field_from_server(table, "records", allow_cache=allow_cache))

    def data_size(self, table, allow_cache=False):
        if self.fetch_info_from_http:
            return int(self.get_field_from_page(table, "Size"))
        else:
            return self._as_int(self.get_field_from_server(table, "size", allow_cache=allow_cache))

    def is_sorted(self, table, allow_cache=False):
        if self.fetch_info_from_http:
            return self.get_field_from_page(table, "Sorted").lower() == "yes"
        else:
            return self.get_field_from_server(table, "sorted", allow_cache=allow_cache) == 1

    def is_empty(self, table, allow_cache=False):
        if not self.fetch_info_from_http:
            count = self.get_field_from_server(table, "records", allow_cache=allow_cache)
            return count is None or count == 0
        """ Parse whether table is empty from html """
        http_content = sh.curl("{0}/debug?info=table&table={1}".format(self.http_server, table)).stdout
        empty_lines = filter(lambda line: line.find("is empty") != -1,  http_content.split("\n"))
        return empty_lines and empty_lines[0].startswith("Table is empty")

    def drop(self, table):
        _check_call("USER=yt MR_USER={0} {1} -server {2} -drop {3}".format(self.mr_user, self.binary, self.server, table), shell=True, timeout=self._light_command_timeout)

    def copy(self, src, dst):
        _check_call("USER=yt MR_USER={0} {1} -server {2} -copy -src {3} -dst {4}".format(self.mr_user, self.binary, self.server, src, dst), shell=True, timeout=self._light_command_timeout)

    def _as_int(self, obj):
        if obj is None:
            return 0
        return int(obj)

    def write(self, table, data):
        command = "MR_USER={0} {1} -server {2} -write {3}".format(self.mr_user, self.binary, self.server, table)
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, shell=True)
        proc.communicate(data)
        if proc.returncode != 0:
            raise subprocess.CalledProcessError("Command '{0}' return non-zero exit status {1}".format(command, proc.returncode))

    def get_read_range_command(self, table):
        if self.proxies:
            return 'curl "http://${{server}}/table/{0}?subkey=1&lenval=1&startindex=${{start}}&endindex=${{end}}"'.format(quote_plus(table))
        else:
            fastbone_str = "-opt net_table=fastbone" if self.fastbone else ""
            shared_tx_str = ("-sharedtransactionid yt_" + generate_uuid()) if self.supports_shared_transactions else ""
            return '{0} MR_USER={1} USER=yt ./{2} -server $server {3} -read {4}:[$start,$end] -lenval -subkey {5}'\
                        .format(self.opts, self.mr_user, self.binary_name, fastbone_str, table, shared_tx_str)

    def get_write_command(self, table):
        fastbone_str = "-opt net_table=fastbone" if self.fastbone else ""
        return "{0} USER=yt MR_USER={1} ./{2} -server {3} {4} -append -lenval -subkey -write {5}"\
                .format(self.opts, self.mr_user, self.binary_name, self.server, fastbone_str, table)

    def run_map(self, command, src, dst, files=None, opts=""):
        if files is None:
            files = []
        shell_command = "MR_USER={0} {1} -server {2} -map '{3}' -src {4} -dst {5} {6} {7}"\
            .format(self.mr_user, self.binary, self.server, command, src, dst, " ".join("-file " + file for file in files), opts)
        _check_call(shell_command, shell=True)

    def remote_copy(self, remote_server, src, dst, mr_user=None):
        if mr_user is None:
            mr_user = self.mr_user
        fastbone_str = "-opt net_table=fastbone" if self.fastbone else ""
        shell_command = "MR_USER={0} {1} -srcserver {2} -server {3} -copy -src {4} -dst {5} {6}"\
            .format(mr_user, self.binary, remote_server, self.server, src, dst, fastbone_str)
        _check_call(shell_command, shell=True)
