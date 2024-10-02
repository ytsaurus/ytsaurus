from __future__ import print_function

from yt.orm.client.client import (
    BatchingOptions,
    get_proto_enum_value_name,
    to_proto_enum_by_number,
)
from yt.orm.library.common import ClientError
from yt.orm.library.cli_helpers import (
    AliasedSubParsersAction,
    OrderByArgument,
    SetUpdateAction,
    SvnVersionAction,
    UniqueStore,
)

try:
    from yt.packages.prettytable import PrettyTable
except ImportError:
    from prettytable import PrettyTable

try:
    from yp.packages.termcolor import colored
except ImportError:
    from termcolor import colored

import yt.yson as yson

try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

from yt.common import YtResponseError, get_value

try:
    from yt.packages.six.moves import map as imap
except ImportError:
    from six.moves import map as imap

from yt.wrapper.cli_helpers import ParseStructuredArgument

from abc import ABCMeta, abstractmethod
from argparse import (
    ArgumentParser,
    ArgumentError,
)
from datetime import (
    datetime,
    timedelta,
)
import os


def get_format_text_dumps(format_name):
    if format_name == "yson":
        return lambda item: yson._dumps_to_native_str(item, yson_format="text")
    if format_name == "json":
        return lambda item: json.dumps(item, encoding="latin1")
    if format_name == "unicode_json":
        return lambda item: json.dumps(yson.yson_to_json(item), ensure_ascii=False)
    raise ArgumentError("Unknown format name '{}'".format(format_name))


def get_format_pretty_dumps(format_name):
    if format_name == "yson":
        return lambda item: yson._dumps_to_native_str(item, yson_format="pretty")
    if format_name == "json":
        return lambda item: json.dumps(yson.yson_to_json(item), indent=4, encoding="latin1")
    if format_name == "unicode_json":
        return lambda item: json.dumps(yson.yson_to_json(item), indent=4, ensure_ascii=False)
    raise ArgumentError("Unknown format name '{}'".format(format_name))


def get_format_dumps(format_name, pretty):
    if pretty:
        return get_format_pretty_dumps(format_name)
    else:
        return get_format_text_dumps(format_name)


def _print_nontabular_result(result, format):
    print(get_format_pretty_dumps(format)(result))


def _print_result(result, selector, format, tabular, is_list):
    if tabular:
        if not selector:
            raise ArgumentError(
                None, "At least one 'selector' must be specified in 'tabular' format"
            )
        if not is_list:
            result = [result]
        table = PrettyTable()
        table.field_names = selector
        for row in result:
            table.add_row(list(imap(get_format_text_dumps(format), row)))
        print(table.get_string())
        if is_list:
            print(len(result), "object(s) selected")
    else:
        _print_nontabular_result(result, format)


def _prepare_selector(selector, selector_positional):
    if selector_positional is not None:
        if selector is None:
            selector = selector_positional
        else:
            selector += selector_positional
    return selector


class ClientCli(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_human_readable_orm_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_event_type(self):
        raise NotImplementedError

    @abstractmethod
    def make_orm_client(self, address, protocol, config):
        raise NotImplementedError

    @abstractmethod
    def find_token(self, allow_receive_token_by_ssh_session):
        raise NotImplementedError

    # Overridable.
    def get_snake_case_orm_name(self):
        return self.get_human_readable_orm_name().lower().replace(" ", "_")

    def get_upper_snake_case_orm_name(self):
        return self.get_snake_case_orm_name().upper()

    def get_validate_connection_object_name(self):
        return "user"

    def get_orm_address_env_name(self):
        return "{}_ADDRESS".format(self.get_upper_snake_case_orm_name())

    def get_orm_protocol_env_name(self):
        return "{}_PROTOCOL".format(self.get_upper_snake_case_orm_name())

    def get_access_control_wiki(self):
        return "https://wiki.yandex-team.ru/{}/".format(self.get_snake_case_orm_name())

    ################################################################################

    def get_masters(self, orm_client, format, pretty):
        print(get_format_dumps(format, pretty)(orm_client.get_masters()))

    def get_object(
        self,
        orm_client,
        object_identity,
        object_type,
        selector,
        selector_positional,
        timestamp,
        timestamp_by_transaction_id,
        transaction_id,
        fetch_timestamps,
        format,
        tabular,
    ):
        selector = _prepare_selector(selector, selector_positional)
        if not selector:
            selector = [""]

        enable_structured_response = False
        options = None
        if fetch_timestamps:
            enable_structured_response = True
            options = dict(fetch_timestamps=True, fetch_values=True)

        if tabular is None:
            tabular = False

        if fetch_timestamps and tabular:
            raise ClientError("Tabular format is incompatible with --fetch-timestamps option")

        _print_result(
            orm_client.get_object(
                object_type,
                object_identity,
                selector,
                timestamp=timestamp,
                timestamp_by_transaction_id=timestamp_by_transaction_id,
                transaction_id=transaction_id,
                options=options,
                enable_structured_response=enable_structured_response,
            ),
            selector,
            format,
            tabular,
            is_list=False,
        )

    def watch_objects(
        self,
        orm_client,
        format,
        **kwargs
    ):
        if kwargs.get("time_limit", None) is not None:
            kwargs["time_limit"] = timedelta(milliseconds=kwargs["time_limit"])
        result = orm_client.watch_objects(request_meta_response=True, **kwargs)
        print(get_format_pretty_dumps(format)(result))

    def select_objects(
        self,
        orm_client,
        object_type,
        filter,
        selector,
        selector_positional,
        timestamp,
        offset,
        limit,
        fetch_timestamps,
        format,
        tabular,
        order_by,
        index,
    ):
        selector = _prepare_selector(selector, selector_positional)

        enable_structured_response = False
        options = None
        if fetch_timestamps:
            enable_structured_response = True
            options = dict(fetch_timestamps=True, fetch_values=True)

        if tabular is None:
            tabular = True

        if fetch_timestamps and tabular:
            raise ClientError("Tabular format is incompatible with --fetch-timestamps option")

        _print_result(
            orm_client.select_objects(
                object_type,
                filter,
                selector,
                timestamp,
                offset,
                limit,
                options=options,
                enable_structured_response=enable_structured_response,
                batching_options=BatchingOptions(
                    max_batch_size=10240,
                    required=False,
                ),
                order_by=order_by,
                index=index,
            ),
            selector,
            format,
            tabular,
            is_list=True,
        )

    def select_object_history(
        self,
        orm_client,
        object_type,
        object_identity,
        selector,
        uuid,
        limit,
        continuation_token,
        interval_begin,
        interval_end,
        descending_time_order,
        prettify_events,
        distinct,
        distinct_by,
        format,
        index_mode,
        filter,
    ):
        options = {}
        if uuid is not None:
            options["uuid"] = uuid
        if limit is not None:
            options["limit"] = limit
        if continuation_token is not None:
            options["continuation_token"] = continuation_token
        options["interval"] = (interval_begin, interval_end)
        options["descending_time_order"] = descending_time_order
        options["distinct"] = distinct
        options["distinct_by"] = distinct_by
        if index_mode is not None:
            options["index_mode"] = index_mode
        if filter:
            options["filter"] = filter

        response = orm_client.select_object_history(
            object_type, object_identity, selector, options=options
        )

        if prettify_events:
            for event in response.get("events", []):
                if "time" in event:
                    event["time"] = str(datetime.fromtimestamp(event["time"] / (1000.0**2)))
                if "event_type" in event:
                    event["event_type"] = get_proto_enum_value_name(
                        to_proto_enum_by_number(self.get_event_type(), event["event_type"]),
                    )

        _print_nontabular_result(response, format)

    def aggregate_objects(
        self, orm_client, object_type, filter, aggregator, group_by, timestamp, format, tabular
    ):
        if tabular is None:
            tabular = True
        _print_result(
            orm_client.aggregate_objects(object_type, group_by, aggregator, filter, timestamp),
            group_by + aggregator,
            format,
            tabular,
            is_list=True,
        )

    def create_object(self, orm_client, object_type, attributes, transaction_id):
        print(orm_client.create_object(object_type, attributes, transaction_id))

    def remove_object(self, orm_client, object_identity, object_type, transaction_id):
        orm_client.remove_object(object_type, object_identity, transaction_id)

    def update_object(
        self, orm_client, object_identity, object_type, set, remove, lock, transaction_id
    ):
        set_updates = None
        if set is not None:
            for set_update in set:
                if set_updates is None:
                    set_updates = []
                set_updates.append(set_update)
        remove_updates = None
        if remove is not None:
            remove_updates = [{"path": path} for path in remove]
        lock_updates = None
        if lock is not None:
            lock_updates = [{"path": path, "lock_type": lock_type} for path, lock_type in lock]
        orm_client.update_object(
            object_type,
            object_identity,
            set_updates=set_updates,
            remove_updates=remove_updates,
            lock_updates=lock_updates,
            transaction_id=transaction_id,
        )

    def generate_timestamp(self, orm_client):
        print(orm_client.generate_timestamp())

    def start_transaction(self, orm_client):
        print(orm_client.start_transaction())

    def commit_transaction(self, orm_client, transaction_id):
        orm_client.commit_transaction(transaction_id)

    def abort_transaction(self, orm_client, transaction_id):
        orm_client.abort_transaction(transaction_id)

    def check_object_permission(self, orm_client, timestamp, format, pretty, **subrequest):
        if format is None:
            format = "yson"
        if pretty is None:
            pretty = True

        subrequests = [subrequest]
        subresponses = orm_client.check_object_permissions(subrequests, timestamp=timestamp)
        if len(subresponses) != 1:
            raise ClientError(
                "Error parsing check-object-permission response: "
                "expected exactly one record, but got {}".format(len(subresponses))
            )

        print(get_format_dumps(format, pretty)(subresponses[0]))

    def get_object_access_allowed_for(self, orm_client, timestamp, format, pretty, **subrequest):
        if format is None:
            format = "yson"
        if pretty is None:
            pretty = True

        subrequests = [subrequest]
        subresponses = orm_client.get_object_access_allowed_for(subrequests, timestamp=timestamp)
        if len(subresponses) != 1:
            raise ClientError(
                "Error parsing get-object-access-allowed-for response: "
                "expected exactly one record, but got {}".format(len(subresponses))
            )

        print(get_format_dumps(format, pretty)(subresponses[0]))

    def get_user_access_allowed_to(self, orm_client, format, pretty, **subrequest):
        if format is None:
            format = "yson"
        if pretty is None:
            pretty = True

        subrequests = [subrequest]
        subresponses = orm_client.get_user_access_allowed_to(subrequests)
        if len(subresponses) != 1:
            raise ClientError(
                "Error parsing get-user-access-allowed-to response: "
                "expected exactly one record, but got {}".format(len(subresponses))
            )

        print(get_format_dumps(format, pretty)(subresponses[0]))

    ################################################################################

    def add_format_argument(self, parser):
        parser.add_argument(
            "--format",
            choices=("yson", "json", "unicode_json"),
            default="yson",
            help="Attention: JSON format uses latin1 encoding to dump binary data, use 'unicode_json' if response has no binary data",
        )

    def configure_tabular_parser(self, parser):
        tabular_parser = parser.add_mutually_exclusive_group(required=False)
        tabular_parser.add_argument("--tabular", dest="tabular", default=None, action="store_true")
        tabular_parser.add_argument(
            "--no-tabular", dest="tabular", default=None, action="store_false"
        )

    def add_pretty_argument(self, parser):
        pretty_parser = parser.add_mutually_exclusive_group(required=False)
        pretty_parser.add_argument("--pretty", dest="pretty", default=None, action="store_true")
        pretty_parser.add_argument("--no-pretty", dest="pretty", default=None, action="store_false")

    def add_get_masters_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser("get-masters", help="Get list of masters", parents=[config_parser])
        parser.set_defaults(func=self.get_masters)
        self.add_format_argument(parser)
        self.add_pretty_argument(parser)

    def add_get_object_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "get-object", help="get object by id", parents=[config_parser], aliases=["get"]
        )
        parser.set_defaults(func=self.get_object)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("selector_positional", nargs="*")
        parser.add_argument("--selector", action="append")
        parser.add_argument("--timestamp", type=int)
        parser.add_argument("--timestamp-by-transaction-id")
        parser.add_argument("--transaction-id", "--transaction_id")
        parser.add_argument("--fetch-timestamps", action="store_true")
        self.add_format_argument(parser)
        self.configure_tabular_parser(parser)

    def add_watch_objects_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "watch-objects", help="watch objects", parents=[config_parser], aliases=["watch"]
        )
        parser.set_defaults(func=self.watch_objects)
        parser.add_argument("object_type")
        parser.add_argument("--start_timestamp", "--start-timestamp", type=int)
        parser.add_argument("--start-from-earliest-offset", action="store_true", default=False)
        parser.add_argument("--skip-trimmed", action="store_true", default=False)
        parser.add_argument("--timestamp", type=int)
        parser.add_argument("--event-count-limit", type=int)
        parser.add_argument("--continuation-token", type=str)
        parser.add_argument("--time-limit", type=int, help="In milliseconds")
        parser.add_argument("--filter", action=UniqueStore)
        parser.add_argument("--selectors", "--selector", action="append")
        parser.add_argument("--watch-log", type=str)
        self.add_format_argument(parser)

    def add_select_objects_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "select-objects",
            help="select objects by filter",
            parents=[config_parser],
            aliases=["select"],
        )
        parser.set_defaults(func=self.select_objects)
        parser.add_argument("object_type")
        parser.add_argument("--filter", action=UniqueStore)
        parser.add_argument("--selector", action="append")
        parser.add_argument("selector_positional", nargs="*")
        parser.add_argument("--timestamp", type=int)
        parser.add_argument("--offset", type=int)
        parser.add_argument("--limit", type=int)
        parser.add_argument("--fetch-timestamps", action="store_true")
        parser.add_argument(
            "--order-by",
            nargs="+",
            action=OrderByArgument,
            help="order objects by {}".format(
                OrderByArgument.format_usage(),
            ),
        )
        parser.add_argument("--index", type=str)
        self.add_format_argument(parser)
        self.configure_tabular_parser(parser)

    def add_select_object_history_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "select-object-history",
            help="select object history",
            parents=[config_parser],
            aliases=["select-history"],
        )
        parser.set_defaults(func=self.select_object_history)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("--selector", action="append")
        parser.add_argument("--uuid")
        parser.add_argument("--limit", type=int)
        parser.add_argument("--continuation-token")
        parser.add_argument("--interval-begin", type=int)
        parser.add_argument("--interval-end", type=int)
        parser.add_argument(
            "--descending-time-order",
            "--descending_time_order",
            action="store_true",
        )
        parser.add_argument(
            "--prettify-events",
            action="store_true",
            help="Convert event meta-information to the human-readable format",
        )
        parser.add_argument(
            "--distinct",
            action="store_true",
            help="Deduplicate consecutive events with the same content due to the given selectors",
        )

        parser.add_argument("--distinct-by", action="append")

        parser.add_argument("--index-mode", help="One of `enabled`, `disabled` or `default`")
        parser.add_argument("--filter")
        self.add_format_argument(parser)

    def add_aggregate_objects_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "aggregate-objects",
            help="aggregate objects",
            parents=[config_parser],
            aliases=["aggregate"],
        )
        parser.set_defaults(func=self.aggregate_objects)
        parser.add_argument("object_type")
        parser.add_argument("--filter")
        parser.add_argument("--aggregator", action="append", default=[])
        parser.add_argument("--group-by", action="append")
        parser.add_argument("--timestamp", type=int)
        self.add_format_argument(parser)
        self.configure_tabular_parser(parser)

    def add_create_object_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "create-object", help="Create object", parents=[config_parser], aliases=["create"]
        )
        parser.set_defaults(func=self.create_object)
        parser.add_argument("object_type")
        parser.add_argument("--attributes", action=ParseStructuredArgument)
        parser.add_argument("--transaction-id", "--transaction_id")

    def add_remove_object_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "remove-object",
            help="remove object by id",
            parents=[config_parser],
            aliases=["remove"],
        )
        parser.set_defaults(func=self.remove_object)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("--transaction-id", "--transaction_id")

    def add_update_object_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "update-object",
            help="Update object by id",
            parents=[config_parser],
            aliases=["update"],
        )
        parser.set_defaults(func=self.update_object)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("--transaction-id", "--transaction_id")
        parser.add_argument("--set", action=SetUpdateAction)
        parser.add_argument("--remove", action="append")
        parser.add_argument("--lock", action="append", nargs=2)

    def add_generate_timestamp_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "generate-timestamp", help="Generate timestamp", parents=[config_parser]
        )
        parser.set_defaults(func=self.generate_timestamp)

    def add_start_transaction_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "start-transaction", help="Start transaction", parents=[config_parser]
        )
        parser.set_defaults(func=self.start_transaction)

    def add_commit_transaction_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "commit-transaction", help="Commit transaction", parents=[config_parser]
        )
        parser.set_defaults(func=self.commit_transaction)
        parser.add_argument("transaction_id")

    def add_abort_transaction_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "abort-transaction", help="Abort transaction", parents=[config_parser]
        )
        parser.set_defaults(func=self.abort_transaction)
        parser.add_argument("transaction_id")

    def add_check_object_permission_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "check-object-permission",
            help="Check object permission",
            parents=[config_parser],
            aliases=["check-permission"],
        )
        parser.set_defaults(func=self.check_object_permission)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("subject_id")
        parser.add_argument("permission")
        parser.add_argument("--attribute-path")
        parser.add_argument("--timestamp", type=int)
        self.add_format_argument(parser)
        self.add_pretty_argument(parser)

    def add_get_object_access_allowed_for_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "get-object-access-allowed-for",
            help="Get subjects for which a given permission to given object is allowed",
            parents=[config_parser],
            aliases=["get-access-allowed-for"],
        )
        parser.set_defaults(func=self.get_object_access_allowed_for)
        parser.add_argument("object_type")
        parser.add_argument("object_identity")
        parser.add_argument("permission")
        parser.add_argument("--attribute-path")
        parser.add_argument("--timestamp", type=int)
        self.add_format_argument(parser)
        self.add_pretty_argument(parser)

    def add_get_user_access_allowed_to_parser(self, subparsers, config_parser):
        parser = subparsers.add_parser(
            "get-user-access-allowed-to",
            help="Get objects of a given type for which given user is granted given permission",
            parents=[config_parser],
        )
        parser.set_defaults(func=self.get_user_access_allowed_to)
        parser.add_argument("user_id")
        parser.add_argument("object_type")
        parser.add_argument("permission")
        parser.add_argument("--attribute-path")
        parser.add_argument("--filter")
        parser.add_argument("--continuation-token")
        parser.add_argument("--limit", type=int)
        self.add_format_argument(parser)
        self.add_pretty_argument(parser)

    def add_parsers(self, subparsers, config_parser):
        self.add_get_masters_parser(subparsers, config_parser)
        self.add_get_object_parser(subparsers, config_parser)
        self.add_watch_objects_parser(subparsers, config_parser)
        self.add_select_objects_parser(subparsers, config_parser)
        self.add_select_object_history_parser(subparsers, config_parser)
        self.add_aggregate_objects_parser(subparsers, config_parser)
        self.add_create_object_parser(subparsers, config_parser)
        self.add_remove_object_parser(subparsers, config_parser)
        self.add_update_object_parser(subparsers, config_parser)
        self.add_generate_timestamp_parser(subparsers, config_parser)
        self.add_start_transaction_parser(subparsers, config_parser)
        self.add_commit_transaction_parser(subparsers, config_parser)
        self.add_abort_transaction_parser(subparsers, config_parser)
        self.add_check_object_permission_parser(subparsers, config_parser)
        self.add_get_object_access_allowed_for_parser(subparsers, config_parser)
        self.add_get_user_access_allowed_to_parser(subparsers, config_parser)

    ################################################################################

    def get_fun_args(self, args):
        func_args = dict(vars(args))
        func_args.pop("svn_version")
        func_args.pop("func")
        func_args.pop("address")
        func_args.pop("protocol")
        func_args.pop("config")
        return func_args

    def validate_connection(self, orm_client):
        try:
            orm_client.select_objects(
                self.get_validate_connection_object_name(), selectors=["/meta/id"], limit=1
            )
        except Exception as ex:
            if isinstance(ex, YtResponseError) and ex.contains_code(109):  # Authentication error.
                host = None
                if ex.inner_errors and "attributes" in ex.inner_errors[0]:
                    host = ex.inner_errors[0]["attributes"].get("host")

                request_id = ex.attributes.get("request_id", None)

                msg = (
                    "Your authentication token was rejected by the server.\n"
                    "Please refer to {} for obtaining a valid token.\n"
                    "Host: {}. Request id: {}, Error: {}.".format(
                        self.get_access_control_wiki(),
                        host,
                        request_id,
                        ex,
                    )
                )
                print(colored(msg, "red"))
            else:
                print(colored(
                    "Error validating connection with {}: {}".format(
                        self.get_human_readable_orm_name(),
                        ex,
                    ),
                    "red",
                ))
            exit(1)

    def main(self):
        config_parser = ArgumentParser(add_help=False)
        config_parser.add_argument(
            "--address",
            help="address of {} backend".format(self.get_human_readable_orm_name()),
        )
        config_parser.add_argument(
            "--protocol",
            help="Protocol to {} ('grpc' or 'http')".format(self.get_human_readable_orm_name()),
        )
        config_parser.add_argument(
            "--config",
            help="Configuration of client in YSON format",
            action=ParseStructuredArgument,
        )

        parser = ArgumentParser(description="Tool for executing commands in {}".format(
            self.get_human_readable_orm_name(),
        ))
        parser.register("action", "parsers", AliasedSubParsersAction)
        parser.add_argument(
            "--svn-version",
            action=SvnVersionAction,
            help="Show program SVN version and exit",
        )
        subparsers = parser.add_subparsers(metavar="command")
        subparsers.required = True

        self.add_parsers(subparsers, config_parser)

        args = parser.parse_args()

        address = get_value(args.address, os.environ.get(self.get_orm_address_env_name()))
        if address is None:
            raise ArgumentError(
                None,
                "Argument --address or environment variable {} must be specified".format(
                    self.get_orm_address_env_name()
                ),
            )
        protocol = get_value(args.protocol, os.environ.get(self.get_orm_protocol_env_name()))
        config = get_value(args.config, {})

        token = self.find_token(config.get("allow_receive_token_by_ssh_session", True))

        if token is not None:
            config["token"] = token

        with self.make_orm_client(address, protocol, config) as orm_client:
            self.validate_connection(orm_client)
            args.func(orm_client, **self.get_fun_args(args))
