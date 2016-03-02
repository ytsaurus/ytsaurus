import yt.logger as logger
from yt.common import YtError, set_pdeathsig, flatten
from yt.wrapper.common import generate_uuid
import yt.json as json

import os
import sh
import subprocess32 as subprocess
from datetime import datetime, timedelta
from urllib import quote_plus

class YamrError(YtError):
    pass

def _check_output(command, silent=False, **kwargs):
    logger.info("Executing command '{}'".format(command))
    result = subprocess.check_output(command, preexec_fn=set_pdeathsig, **kwargs)
    logger.info("Command '{}' successfully executed".format(command))
    return result

def _check_call(command, silent=False, **kwargs):
    logger.info("Executing command '{}'".format(command))
    timeout = kwargs.pop('timeout', None)
    proc = subprocess.Popen(command, stderr=subprocess.PIPE, preexec_fn=set_pdeathsig, **kwargs)
    try:
        _, stderrdata = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise

    # NB: We need to convert stderr string to utf-8, because we can try to dump it to json later.
    try:
        stderrdata = stderrdata.decode("cp1251").encode("utf-8")
    except Exception:
        pass

    if proc.returncode != 0:
        raise YamrError("Command '{0}' failed".format(command), code=proc.returncode, attributes={"stderrs": stderrdata})

    logger.info("Command '{}' successfully executed".format(command))

class Yamr(object):
    def __init__(self, binary, server, server_port, http_port, name=None, proxies=None, proxy_port=None, fetch_info_from_http=False, mr_user="tmp", opts="", timeout=None, max_failed_jobs=None, scheduler_info_update_period=5.0):
        self.binary = binary
        self.binary_name = os.path.basename(binary)
        self.name = name
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

        if timeout is None:
            timeout = 60.0
        self._light_command_timeout = timeout

        if max_failed_jobs is None:
            max_failed_jobs = 100
        self.max_failed_jobs = max_failed_jobs

        self.scheduler_info_update_period = scheduler_info_update_period
        self._last_update_time_of_scheduler_info = None
        self.scheduler_info = {}

        self.supports_shared_transactions = \
            subprocess.call("{0} --help | grep sharedtransaction >/dev/null".format(self.binary), shell=True) == 0

        self.supports_read_snapshots = \
            subprocess.call("{0} --help | grep mkreadsnapshot >/dev/null".format(self.binary), shell=True) == 0

        logger.info("Yamr options configured (binary: %s, server: %s, http_server: %s, proxies: [%s])",
                    self.binary, self.server, self.http_server, ", ".join(self.proxies))

    def _make_address(self, address, port):
        if ":" in address and address.rsplit(":", 1)[1].isdigit():
            address = address.rsplit(":", 1)[0]
        return "{0}:{1}".format(address, port)

    def _make_fastbone(self, fastbone):
        return "-opt net_table=fastbone" if fastbone else ""

    def get_field_from_page(self, table, field):
        """ Extract value of given field from http page of the table """
        http_content = sh.curl("{0}/debug?info=table&table={1}".format(self.http_server, table)).stdout
        records_line = filter(lambda line: line.find(field) != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace("<br>", "").replace("<b>", "").replace(",", "")
        return records_line.split(field + ":")[1].split()[0]

    def list(self, prefix):
        output = _check_output(
            '{0} -server {1} -list -prefix "{2}" -jsonoutput'\
                .format(self.binary, self.server, prefix),
            timeout=self._light_command_timeout,
            shell=True)
        return json.loads_as_bytes(output)

    def get_field_from_server(self, table, field, allow_cache):
        if not allow_cache or table not in self.cache:
            output = _check_output(
                '{0} -server {1} -list -prefix "{2}" -jsonoutput'\
                    .format(self.binary, self.server, table),
                timeout=self._light_command_timeout,
                shell=True)
            table_info = filter(lambda obj: obj["name"] == table, json.loads_as_bytes(output))
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

    def get_compression_codec(self, table, allow_cache=False):
        return self.get_field_from_server(table, "compression_algo", allow_cache=allow_cache)

    def is_directory(self, path):
        if path.endswith("/"):
            path = path[:-1]
        output = _check_output(
            '{0} -server {1} -list -prefix "{2}" -jsonoutput'.format(self.binary, self.server, path),
            timeout=self._light_command_timeout,
            shell=True)
        listing = json.loads_as_bytes(output)
        for entry in listing:
            if entry["name"].startswith(path + "/"):
                return True
        return False

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
        _check_call('USER=yt MR_USER={0} {1} -server {2} -drop "{3}"'.format(self.mr_user, self.binary, self.server, table), shell=True, timeout=self._light_command_timeout)

    def copy(self, src, dst):
        _check_call('USER=yt MR_USER={0} {1} -server {2} -copy -src "{3}" -dst "{4}"'.format(self.mr_user, self.binary, self.server, src, dst), shell=True, timeout=self._light_command_timeout)

    def _as_int(self, obj):
        if obj is None:
            return 0
        return int(obj)

    def write(self, table, data):
        command = '{0} MR_USER={1} {2} -server {3} -write "{4}"'.format(self.opts, self.mr_user, self.binary, self.server, table)
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=set_pdeathsig, shell=True)
        _, stderr = proc.communicate(data)
        if proc.returncode != 0:
            error = YamrError("Command '{0}' failed".format(command))
            error.inner_errors = [YamrError(stderr, proc.returncode)]
            raise error

    def create_read_range_commands(self, ranges, table, fastbone, transaction_id=None, timeout=None, enable_logging=False):
        commands = []
        if transaction_id is None:
            transaction_id = "yt_" + generate_uuid()

        timeout_str = ""
        if timeout is not None:
            timeout_str = "timeout {0}s".format(timeout)
        for i, range in enumerate(ranges):
            start, end = range
            if self.proxies:
                logging_str = ""
                if enable_logging:
                    logging_str = "-v"
                # NB: shared transasction is not supported.
                command = '{timeout} curl {logging} "http://{proxy}/table/{1}?subkey=1&lenval=1&startindex={2}&endindex={3}"'\
                        .format(
                            timeout=timeout,
                            logging=logging_str,
                            proxy=self.proxies[i % len(self.proxies)],
                            table=quote_plus(table),
                            start=start,
                            end=end)
            else:
                logging_str = ""
                if enable_logging:
                    logging_str = "-stderrlevel 11"
                shared_tx_str = ("-sharedtransactionid " + transaction_id) if self.supports_shared_transactions else ""
                command = '{timeout} {opts} MR_USER={mr_user} USER=yt '\
                          './{binary} -server {server} {fastbone_option} -read "{table}:[{start},{end}]" -lenval -subkey {shared_tx} {logging}\n'\
                        .format(
                            timeout=timeout_str,
                            opts=self.opts,
                            mr_user=self.mr_user,
                            binary=self.binary_name,
                            server=self.server,
                            fastbone_option=self._make_fastbone(fastbone),
                            table=table,
                            start=start,
                            end=end,
                            shared_tx=shared_tx_str,
                            logging=logging_str)
            commands.append(command)
        return commands

    def get_write_command(self, table, fastbone):
        return '{0} USER=yt MR_USER={1} ./{2} -server {3} {4} -append -lenval -subkey -write "{5}"'\
                .format(self.opts, self.mr_user, self.binary_name, self.server, self._make_fastbone(fastbone), table)

    def run_sort(self, src, dst, opts=""):
        shell_command = 'MR_USER={0} {1} -server {2} -sort -src "{3}" -dst "{4}" -maxjobfails {5} {6}'\
            .format(self.mr_user, self.binary, self.server, src, dst, self.max_failed_jobs, opts)
        _check_call(shell_command, shell=True)

    def run_map(self, command, src, dst, files=None, opts=""):
        if files is None:
            files = []

        dst_args = " ".join('-dst "{0}"'.format(table) for table in flatten(dst))
        src_args = " ".join('-src "{0}"'.format(table) for table in flatten(src))
        file_args = " ".join('-file "{0}"'.format(file_) for file_ in flatten(files))

        shell_command = 'MR_USER={0} {1} -server {2} -map "{3}" {4} {5} -maxjobfails {6} {7} {8}'\
            .format(self.mr_user, self.binary, self.server, command, src_args, dst_args,
                    self.max_failed_jobs, file_args, opts)

        _check_call(shell_command, shell=True)

    def make_read_snapshot(self, table, finish_timeout=7200):
        transaction_id = "yt_" + generate_uuid()
        shell_command = 'MR_USER={0} {1} -server {2} -mkreadsnapshot "{3}" -sharedtransactionid {4} -finishtimeout {5}'\
            .format(self.mr_user, self.binary, self.server, table, transaction_id, finish_timeout)
        _check_call(shell_command, shell=True)
        return transaction_id

    def remote_copy(self, remote_server, src, dst, fastbone):
        shell_command = 'MR_USER={0} {1} -srcserver {2} -server {3} -copy -src "{4}" -dst "{5}" {6}'\
            .format(self.mr_user, self.binary, remote_server, self.server, src, dst, self._make_fastbone(fastbone))
        _check_call(shell_command, shell=True)

    def get_scheduler_info(self):
        if self._last_update_time_of_scheduler_info is None or \
                datetime.now() - self._last_update_time_of_scheduler_info > timedelta(seconds=self.scheduler_info_update_period):
            self._last_update_time_of_scheduler_info = datetime.now()
            try:
                scheduler_info = sh.curl("{0}/json?info=scheduler".format(self.http_server), "--max-time", 1, insecure=True, location=True).stdout
                try:
                    self.scheduler_info = json.loads_as_bytes(scheduler_info)
                except ValueError:
                    self.scheduler_info = {}
            except Exception as err:
                logger.warning("Error occured (%s: %s) while requesting scheduler info from %s", str(type(err)), str(err), self.http_server)
        return self.scheduler_info

    def get_operations(self):
        return self.get_scheduler_info().get("scheduler", {}).get("operationList", [])
