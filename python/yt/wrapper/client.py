from common import update
from default_config import get_default_config
from cypress_commands import set, get, list, exists, remove, search, mkdir, copy, move, link, get_type, create, \
                          has_attribute, get_attribute, set_attribute, list_attributes, find_free_subpath
from acl_commands import check_permission, add_member, remove_member
from table_commands import create_table, create_temp_table, write_table, read_table, \
                           records_count, is_sorted, is_empty, \
                           run_erase, run_sort, run_merge, \
                           run_map, run_reduce, run_map_reduce, run_remote_copy, \
                           mount_table, unmount_table, remount_table, reshard_table, select_rows
from operation_commands import get_operation_state, abort_operation, suspend_operation, resume_operation
from file_commands import read_file, write_file, upload_file, smart_upload_file
from transaction_commands import start_transaction, abort_transaction, commit_transaction, ping_transaction
from http import get_user_name
from transaction import Transaction, PingableTransaction, PingTransaction
from lock import lock

# XXX(ignat): rename?
class Yt(object):
    def __init__(self, proxy=None, token=None, config=None):
        self.config = get_default_config()
        if config is not None:
            self.config = update(self.config, config)

        if proxy is not None:
            self.config["proxy"]["url"] = proxy
        if token is not None:
            self.config["token"] = token

        # TODO(ignat): It is copy-paste of config option. I need avoid it in some way.
        self.RETRY = None
        self.SPEC = None
        self.MUTATION_ID = None
        self.TRACE = None
        self.TRANSACTION = "0-0-0-0"
        self.PING_ANCESTOR_TRANSACTIONS = False
        self._ENABLE_READ_TABLE_CHAOS_MONKEY = False
        self._ENABLE_HTTP_CHAOS_MONKEY = False
        self._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

        self._transaction_stack = None
        self._banned_proxies = {}
        self._ip_configured = False
        self._driver = None

        # Cache for API version (to check it only once)
        self._api_version = None
        self._commands = None

    def get_user_name(self, *args, **kwargs):
        return get_user_name(*args, client=self, **kwargs)

    def set(self, *args, **kwargs):
        return set(*args, client=self, **kwargs)

    def get(self, *args, **kwargs):
        return get(*args, client=self, **kwargs)

    def list(self, *args, **kwargs):
        return list(*args, client=self, **kwargs)

    def exists(self, *args, **kwargs):
        return exists(*args, client=self, **kwargs)

    def remove(self, *args, **kwargs):
        return remove(*args, client=self, **kwargs)

    def search(self, *args, **kwargs):
        return search(*args, client=self, **kwargs)

    def mkdir(self, *args, **kwargs):
        return mkdir(*args, client=self, **kwargs)

    def copy(self, *args, **kwargs):
        return copy(*args, client=self, **kwargs)

    def move(self, *args, **kwargs):
        return move(*args, client=self, **kwargs)

    def link(self, *args, **kwargs):
        return link(*args, client=self, **kwargs)

    def get_type(self, *args, **kwargs):
        return get_type(*args, client=self, **kwargs)

    def create(self, *args, **kwargs):
        return create(*args, client=self, **kwargs)

    def has_attribute(self, *args, **kwargs):
        return has_attribute(*args, client=self, **kwargs)

    def get_attribute(self, *args, **kwargs):
        return get_attribute(*args, client=self, **kwargs)

    def set_attribute(self, *args, **kwargs):
        return set_attribute(*args, client=self, **kwargs)

    def list_attributes(self, *args, **kwargs):
        return list_attributes(*args, client=self, **kwargs)

    def find_free_subpath(self, *args, **kwargs):
        return find_free_subpath(*args, client=self, **kwargs)

    def check_permission(self, *args, **kwargs):
        return check_permission(*args, client=self, **kwargs)

    def add_member(self, *args, **kwargs):
        return add_member(*args, client=self, **kwargs)

    def remove_member(self, *args, **kwargs):
        return remove_member(*args, client=self, **kwargs)

    def create_table(self, *args, **kwargs):
        return create_table(*args, client=self, **kwargs)

    def create_temp_table(self, *args, **kwargs):
        return create_temp_table(*args, client=self, **kwargs)

    def write_table(self, *args, **kwargs):
        return write_table(*args, client=self, **kwargs)

    def read_table(self, *args, **kwargs):
        return read_table(*args, client=self, **kwargs)

    def records_count(self, *args, **kwargs):
        return records_count(*args, client=self, **kwargs)

    def is_sorted(self, *args, **kwargs):
        return is_sorted(*args, client=self, **kwargs)

    def is_empty(self, *args, **kwargs):
        return is_empty(*args, client=self, **kwargs)

    def run_erase(self, *args, **kwargs):
        return run_erase(*args, client=self, **kwargs)

    def run_sort(self, *args, **kwargs):
        return run_sort(*args, client=self, **kwargs)

    def run_merge(self, *args, **kwargs):
        return run_merge(*args, client=self, **kwargs)

    def run_map(self, *args, **kwargs):
        return run_map(*args, client=self, **kwargs)

    def run_reduce(self, *args, **kwargs):
        return run_reduce(*args, client=self, **kwargs)

    def run_map_reduce(self, *args, **kwargs):
        return run_map_reduce(*args, client=self, **kwargs)

    def run_remote_copy(self, *args, **kwargs):
        return run_remote_copy(*args, client=self, **kwargs)

    def mount_table(self, *args, **kwargs):
        return mount_table(*args, client=self, **kwargs)

    def unmount_table(self, *args, **kwargs):
        return unmount_table(*args, client=self, **kwargs)

    def remount_table(self, *args, **kwargs):
        return remount_table(*args, client=self, **kwargs)

    def reshard_table(self, *args, **kwargs):
        return reshard_table(*args, client=self, **kwargs)

    def select_rows(self, *args, **kwargs):
        return select_rows(*args, client=self, **kwargs)

    def get_operation_state(self, *args, **kwargs):
        return get_operation_state(*args, client=self, **kwargs)

    def abort_operation(self, *args, **kwargs):
        return abort_operation(*args, client=self, **kwargs)

    def suspend_operation(self, *args, **kwargs):
        return suspend_operation(*args, client=self, **kwargs)

    def resume_operation(self, *args, **kwargs):
        return resume_operation(*args, client=self, **kwargs)

    def read_file(self, *args, **kwargs):
        return read_file(*args, client=self, **kwargs)

    def write_file(self, *args, **kwargs):
        return write_file(*args, client=self, **kwargs)

    def download_file(self, *args, **kwargs):
        """Deprecated. For backward compatibility only"""
        return read_file(*args, client=self, **kwargs)

    def upload_file(self, *args, **kwargs):
        """Deprecated. For backward compatibility only"""
        return upload_file(*args, client=self, **kwargs)

    def smart_upload_file(self, *args, **kwargs):
        return smart_upload_file(*args, client=self, **kwargs)

    def start_transaction(self, *args, **kwargs):
        return start_transaction(*args, client=self, **kwargs)

    def abort_transaction(self, *args, **kwargs):
        return abort_transaction(*args, client=self, **kwargs)

    def commit_transaction(self, *args, **kwargs):
        return commit_transaction(*args, client=self, **kwargs)

    def ping_transaction(self, *args, **kwargs):
        return ping_transaction(*args, client=self, **kwargs)

    def lock(self, *args, **kwargs):
        return lock(*args, client=self, **kwargs)

    def Transaction(self, *args, **kwargs):
        return Transaction(*args, client=self, **kwargs)

    def PingableTransaction(self, *args, **kwargs):
        return PingableTransaction(*args, client=self, **kwargs)

    def PingTransaction(self, *args, **kwargs):
        return PingTransaction(*args, client=self, **kwargs)
