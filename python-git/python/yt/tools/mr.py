import yt.logger as logger

import sh
import subprocess
import simplejson as json

class Mr(object):
    def __init__(self, binary, server, server_port, http_port, proxies, proxy_port, fetch_info_from_http, cache=True, mr_user="tmp"):
        self.binary = binary
        self.server = self._make_address(server, server_port)
        self.http_server = self._make_address(server, http_port)
        if proxies is None:
            self.proxies = []
        else:
            self.proxies = map(lambda proxy: self._make_address(proxy, proxy_port), proxies)
        self.fetch_info_from_http = fetch_info_from_http

        self.cache = {}
        self.use_cache = cache
        self.mr_user = mr_user

        self.supports_shared_transactions = \
            subprocess.call("{} --help | grep sharedtransaction &>/dev/null".format(self.binary), shell=True) == 0

        logger.info("Yamr options configured (binary: %s, server: %s, http_server: %s, proxies: [%s])",
                    self.binary, self.server, self.http_server, ", ".join(self.proxies))

    
    def _make_address(self, address, port):
        if ":" in address and address.rsplit(":", 1)[1].isdigit():
            address = address.rsplit(":", 1)[0]
        return "{}:{}".format(address, port)


    def get_field_from_page(self, table, field):
        """ Extract value of given field from http page of the table """
        http_content = sh.curl("{}/debug?info=table&table={}".format(self.http_server, table)).stdout
        records_line = filter(lambda line: line.find(field) != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace("<br>", "").replace("<b>", "").replace(",", "")
        return records_line.split(field + ":")[1].split()[0]

    def get_field_from_server(self, table, field):
        if not self.use_cache or table not in self.cache:
            output = subprocess.check_output(
                "{} -server {} -list -prefix {} -jsonoutput"\
                    .format(self.binary, self.server, table),
                shell=True)
            table_info = filter(lambda obj: obj["name"] == table, json.loads(output))
            if table_info:
                self.cache[table] = table_info[0]
            else:
                self.cache[table] = None
        
        if self.cache[table] is None:
            return None
        
        return self.cache[table].get(field, None)

    def records_count(self, table):
        if self.fetch_info_from_http:
            return int(self.get_field_from_page(table, "Records"))
        else:
            return self._as_int(self.get_field_from_server(table, "records"))
    
    def data_size(self, table):
        if self.fetch_info_from_http:
            return int(self.get_field_from_page(table, "Size"))
        else:
            return self._as_int(self.get_field_from_server(table, "size"))

    def is_sorted(self, table):
        if self.fetch_info_from_http:
            return self.get_field_from_page(table, "Sorted").lower() == "yes"
        else:
            return self.get_field_from_server(table, "sorted") == 1
    
    def is_empty(self, table):
        if not self.fetch_info_from_http:
            count = self.get_field_from_server(table, "records")
            return count is None or count == 0
        """ Parse whether table is empty from html """
        http_content = sh.curl("{}/debug?info=table&table={}".format(self.http_server, table)).stdout
        empty_lines = filter(lambda line: line.find("is empty") != -1,  http_content.split("\n"))
        return empty_lines and empty_lines[0].startswith("Table is empty")

    def drop(self, table):
        subprocess.check_call("USER=yt MR_USER={} {} -server {} -drop {}".format(self.mr_user, self.binary, self.server, table), shell=True)

    def copy(self, src, dst):
        subprocess.check_call("USER=yt MR_USER={} {} -server {} -copy -src {} -dst {}".format(self.mr_user, self.binary, self.server, src, dst), shell=True)

    def _as_int(self, obj):
        if obj is None:
            return 0
        return int(obj)

    
