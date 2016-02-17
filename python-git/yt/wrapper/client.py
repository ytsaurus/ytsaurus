from common import update
from default_config import get_default_config
from client_state import ClientState
from cypress_commands import set, get, list, exists, remove, search, mkdir, copy, move, link, get_type, create, \
                          has_attribute, get_attribute, set_attribute, list_attributes, find_free_subpath
from acl_commands import check_permission, add_member, remove_member
from table_commands import create_table, create_temp_table, write_table, read_table, \
                           row_count, is_sorted, is_empty, \
                           run_erase, run_sort, run_merge, \
                           run_map, run_reduce, run_join_reduce, run_map_reduce, run_remote_copy, \
                           mount_table, alter_table, unmount_table, remount_table, reshard_table, \
                           select_rows, lookup_rows, insert_rows, delete_rows
from operation_commands import get_operation_state, abort_operation, suspend_operation, resume_operation, \
                               complete_operation
from file_commands import read_file, write_file, upload_file, smart_upload_file
from transaction_commands import start_transaction, abort_transaction, commit_transaction, ping_transaction
from http import get_user_name
from transaction import Transaction, PingableTransaction, PingTransaction
from lock import lock
from table import TempTable
from job_commands import run_job_shell

import functools

# XXX(ignat): rename?
class Yt(ClientState):
    def __init__(self, proxy=None, token=None, config=None):
        ClientState.__init__(self)

        self.config = get_default_config()
        if config is not None:
            self.config = update(self.config, config)

        if proxy is not None:
            self.config["proxy"]["url"] = proxy
        if token is not None:
            self.config["token"] = token

    @functools.wraps(get_user_name)
    def get_user_name(self, *args, **kwargs):
        return get_user_name(*args, client=self, **kwargs)

    @functools.wraps(set)
    def set(self, *args, **kwargs):
        return set(*args, client=self, **kwargs)

    @functools.wraps(get)
    def get(self, *args, **kwargs):
        return get(*args, client=self, **kwargs)

    @functools.wraps(list)
    def list(self, *args, **kwargs):
        return list(*args, client=self, **kwargs)

    @functools.wraps(exists)
    def exists(self, *args, **kwargs):
        return exists(*args, client=self, **kwargs)

    @functools.wraps(remove)
    def remove(self, *args, **kwargs):
        return remove(*args, client=self, **kwargs)

    @functools.wraps(search)
    def search(self, *args, **kwargs):
        return search(*args, client=self, **kwargs)

    @functools.wraps(mkdir)
    def mkdir(self, *args, **kwargs):
        return mkdir(*args, client=self, **kwargs)

    @functools.wraps(copy)
    def copy(self, *args, **kwargs):
        return copy(*args, client=self, **kwargs)

    @functools.wraps(move)
    def move(self, *args, **kwargs):
        return move(*args, client=self, **kwargs)

    @functools.wraps(link)
    def link(self, *args, **kwargs):
        return link(*args, client=self, **kwargs)

    @functools.wraps(get_type)
    def get_type(self, *args, **kwargs):
        return get_type(*args, client=self, **kwargs)

    @functools.wraps(create)
    def create(self, *args, **kwargs):
        return create(*args, client=self, **kwargs)

    @functools.wraps(has_attribute)
    def has_attribute(self, *args, **kwargs):
        return has_attribute(*args, client=self, **kwargs)

    @functools.wraps(get_attribute)
    def get_attribute(self, *args, **kwargs):
        return get_attribute(*args, client=self, **kwargs)

    @functools.wraps(set_attribute)
    def set_attribute(self, *args, **kwargs):
        return set_attribute(*args, client=self, **kwargs)

    @functools.wraps(list_attributes)
    def list_attributes(self, *args, **kwargs):
        return list_attributes(*args, client=self, **kwargs)

    @functools.wraps(find_free_subpath)
    def find_free_subpath(self, *args, **kwargs):
        return find_free_subpath(*args, client=self, **kwargs)

    @functools.wraps(check_permission)
    def check_permission(self, *args, **kwargs):
        return check_permission(*args, client=self, **kwargs)

    @functools.wraps(add_member)
    def add_member(self, *args, **kwargs):
        return add_member(*args, client=self, **kwargs)

    @functools.wraps(remove_member)
    def remove_member(self, *args, **kwargs):
        return remove_member(*args, client=self, **kwargs)

    @functools.wraps(create_table)
    def create_table(self, *args, **kwargs):
        return create_table(*args, client=self, **kwargs)

    @functools.wraps(create_temp_table)
    def create_temp_table(self, *args, **kwargs):
        return create_temp_table(*args, client=self, **kwargs)

    @functools.wraps(write_table)
    def write_table(self, *args, **kwargs):
        return write_table(*args, client=self, **kwargs)

    @functools.wraps(read_table)
    def read_table(self, *args, **kwargs):
        return read_table(*args, client=self, **kwargs)

    def records_count(self, *args, **kwargs):
        return row_count(*args, client=self, **kwargs)

    @functools.wraps(row_count)
    def row_count(self, *args, **kwargs):
        return row_count(*args, client=self, **kwargs)

    @functools.wraps(is_sorted)
    def is_sorted(self, *args, **kwargs):
        return is_sorted(*args, client=self, **kwargs)

    @functools.wraps(is_empty)
    def is_empty(self, *args, **kwargs):
        return is_empty(*args, client=self, **kwargs)

    @functools.wraps(run_erase)
    def run_erase(self, *args, **kwargs):
        return run_erase(*args, client=self, **kwargs)

    @functools.wraps(run_sort)
    def run_sort(self, *args, **kwargs):
        return run_sort(*args, client=self, **kwargs)

    @functools.wraps(run_merge)
    def run_merge(self, *args, **kwargs):
        return run_merge(*args, client=self, **kwargs)

    @functools.wraps(run_map)
    def run_map(self, *args, **kwargs):
        return run_map(*args, client=self, **kwargs)

    @functools.wraps(run_reduce)
    def run_reduce(self, *args, **kwargs):
        return run_reduce(*args, client=self, **kwargs)

    @functools.wraps(run_join_reduce)
    def run_join_reduce(self, *args, **kwargs):
        return run_join_reduce(*args, client=self, **kwargs)

    @functools.wraps(run_map_reduce)
    def run_map_reduce(self, *args, **kwargs):
        return run_map_reduce(*args, client=self, **kwargs)

    @functools.wraps(run_remote_copy)
    def run_remote_copy(self, *args, **kwargs):
        return run_remote_copy(*args, client=self, **kwargs)

    @functools.wraps(mount_table)
    def mount_table(self, *args, **kwargs):
        return mount_table(*args, client=self, **kwargs)

    @functools.wraps(alter_table)
    def alter_table(self, *args, **kwargs):
        return alter_table(*args, client=self, **kwargs)

    @functools.wraps(unmount_table)
    def unmount_table(self, *args, **kwargs):
        return unmount_table(*args, client=self, **kwargs)

    @functools.wraps(remount_table)
    def remount_table(self, *args, **kwargs):
        return remount_table(*args, client=self, **kwargs)

    @functools.wraps(reshard_table)
    def reshard_table(self, *args, **kwargs):
        return reshard_table(*args, client=self, **kwargs)

    @functools.wraps(select_rows)
    def select_rows(self, *args, **kwargs):
        return select_rows(*args, client=self, **kwargs)

    @functools.wraps(lookup_rows)
    def lookup_rows(self, *args, **kwargs):
        return lookup_rows(*args, client=self, **kwargs)

    @functools.wraps(insert_rows)
    def insert_rows(self, *args, **kwargs):
        return insert_rows(*args, client=self, **kwargs)

    @functools.wraps(delete_rows)
    def delete_rows(self, *args, **kwargs):
        return delete_rows(*args, client=self, **kwargs)

    @functools.wraps(get_operation_state)
    def get_operation_state(self, *args, **kwargs):
        return get_operation_state(*args, client=self, **kwargs)

    @functools.wraps(abort_operation)
    def abort_operation(self, *args, **kwargs):
        return abort_operation(*args, client=self, **kwargs)

    @functools.wraps(suspend_operation)
    def suspend_operation(self, *args, **kwargs):
        return suspend_operation(*args, client=self, **kwargs)

    @functools.wraps(resume_operation)
    def resume_operation(self, *args, **kwargs):
        return resume_operation(*args, client=self, **kwargs)

    @functools.wraps(complete_operation)
    def complete_operation(self, *args, **kwargs):
        return complete_operation(*args, client=self, **kwargs)

    @functools.wraps(read_file)
    def read_file(self, *args, **kwargs):
        return read_file(*args, client=self, **kwargs)

    @functools.wraps(write_file)
    def write_file(self, *args, **kwargs):
        return write_file(*args, client=self, **kwargs)

    def download_file(self, *args, **kwargs):
        """Deprecated. For backward compatibility only"""
        return read_file(*args, client=self, **kwargs)

    def upload_file(self, *args, **kwargs):
        """Deprecated. For backward compatibility only"""
        return upload_file(*args, client=self, **kwargs)

    @functools.wraps(smart_upload_file)
    def smart_upload_file(self, *args, **kwargs):
        return smart_upload_file(*args, client=self, **kwargs)

    @functools.wraps(start_transaction)
    def start_transaction(self, *args, **kwargs):
        return start_transaction(*args, client=self, **kwargs)

    @functools.wraps(abort_transaction)
    def abort_transaction(self, *args, **kwargs):
        return abort_transaction(*args, client=self, **kwargs)

    @functools.wraps(commit_transaction)
    def commit_transaction(self, *args, **kwargs):
        return commit_transaction(*args, client=self, **kwargs)

    @functools.wraps(ping_transaction)
    def ping_transaction(self, *args, **kwargs):
        return ping_transaction(*args, client=self, **kwargs)

    @functools.wraps(lock)
    def lock(self, *args, **kwargs):
        return lock(*args, client=self, **kwargs)

    @functools.wraps(Transaction)
    def Transaction(self, *args, **kwargs):
        return Transaction(*args, client=self, **kwargs)

    @functools.wraps(PingableTransaction)
    def PingableTransaction(self, *args, **kwargs):
        return PingableTransaction(*args, client=self, **kwargs)

    @functools.wraps(PingTransaction)
    def PingTransaction(self, *args, **kwargs):
        return PingTransaction(*args, client=self, **kwargs)

    @functools.wraps(TempTable)
    def TempTable(self, *args, **kwargs):
        return TempTable(*args, client=self, **kwargs)

    @functools.wraps(run_job_shell)
    def run_job_shell(self, *args, **kwargs):
        return run_job_shell(*args, client=self, **kwargs)
