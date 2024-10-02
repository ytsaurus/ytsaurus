from yt.orm.library.common import ClientError

import yt.yson as yson

from yt.common import update, get_value

import argparse


def get_svn_version():
    try:
        import library.python.svn_version as svn_version

        return svn_version.svn_version()
    except ImportError:
        return "Svn version is not supported"


class SetUpdateAction(argparse.Action):
    @staticmethod
    def format_usage():
        return "<path> <value> [{recursive=<bool>}]"

    def __init__(self, option_strings, dest, nargs="+", help=None, **kwargs):
        if help is None:
            help = self.format_usage()

        assert nargs == "+", "Set update argument accepts from 2 to 3 subarguments, but requirement \"{}\" is provided".format(nargs)
        super(SetUpdateAction, self).__init__(option_strings, dest, nargs=nargs, help=help, **kwargs)

    def __call__(self, parser, namespace, values, *args, **kwargs):
        if len(values) not in (2, 3):
            raise ClientError("Set update usage {}, but got {} subargument(s)".format(
                self.format_usage(),
                len(values),
            ))
        set_update = dict(path=values[0])
        try:
            set_update["value"] = yson._loads_from_native_str(values[1])
        except yson.YsonError as error:
            raise ClientError(
                "Failed to parse set update value for path \"{}\"".format(values[0]),
                inner_errors=[error],
            )
        if len(values) > 2:
            try:
                set_update_options = yson._loads_from_native_str(values[2])
            except yson.YsonError as error:
                raise ClientError(
                    "Failed to parse set update yson options for path \"{}\"".format(values[0]),
                    inner_errors=[error],
                )
            if not isinstance(set_update_options, yson.YsonMap):
                raise ClientError("Set update options must be of a map type for path \"{}\"".format(
                    values[0],
                ))
            set_update["recursive"] = set_update_options.pop("recursive", False)
            if len(set_update_options) != 0:
                raise ClientError("Set update options contains excessive key(s) {} for path \"{}\"".format(
                    set_update_options.keys(),
                    values[0],
                ))
        argument_values = get_value(getattr(namespace, self.dest), [])
        argument_values.append(set_update)
        setattr(namespace, self.dest, argument_values)


# Builtin argparse version action does not format output properly.
class SvnVersionAction(argparse.Action):
    def __init__(self, option_strings, dest, nargs=0, **kwargs):
        # 0 instead of None is intentional: None is interpreted as 1 by default.
        assert nargs == 0, "SVN version mode does not accept arguments, but requirement \"{}\" is provided".format(nargs)
        super(SvnVersionAction, self).__init__(option_strings, dest, nargs=nargs, **kwargs)

    def __call__(self, *args, **kwargs):
        print(get_svn_version())
        exit(0)


# Aliases argument is not supported for python2, so we implemented it manually.
class AliasedSubParsersAction(argparse._SubParsersAction):
    class _AliasedPseudoAction(argparse.Action):
        def __init__(self, name, aliases, help):
            dest = name
            if aliases:
                dest += " (%s)" % ",".join(aliases)
            sup = super(AliasedSubParsersAction._AliasedPseudoAction, self)
            sup.__init__(option_strings=[], dest=dest, help=help)

    def add_parser(self, name, **kwargs):
        if "aliases" in kwargs:
            aliases = kwargs["aliases"]
            del kwargs["aliases"]
        else:
            aliases = []

        parser = super(AliasedSubParsersAction, self).add_parser(name, **kwargs)

        # Make the aliases work.
        for alias in aliases:
            self._name_parser_map[alias] = parser
        # Make the help text reflect them, first removing old help entry.
        if "help" in kwargs:
            help = kwargs.pop("help")
            self._choices_actions.pop()
            pseudo_action = self._AliasedPseudoAction(name, aliases, help)
            self._choices_actions.append(pseudo_action)

        return parser


class ParseMapFragmentArgument(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Arguments specified multiple times are merged into a single dict.
        old_value = get_value(getattr(namespace, self.dest), {})
        new_value = update(old_value, yson._loads_from_native_str(values, yson_type="map_fragment"))
        setattr(namespace, self.dest, new_value)


class ReplicaArgument(argparse.Action):
    @staticmethod
    def format_usage():
        return "{yt-proxy=<yt-proxy>}"

    def __call__(self, parser, namespace, values, option_string):
        def raise_usage(error):
            raise ClientError(
                "{}, argument replica usage {}".format(error, self.format_usage()),
            )

        replicas = get_value(getattr(namespace, self.dest), [])
        try:
            replica = yson._loads_from_native_str(values)
        except Exception:
            raise_usage("parse error")
        if not isinstance(replica, yson.YsonMap):
            raise_usage("non-map type")
        if "yt-proxy" not in replica:
            raise_usage("missing yt-proxy")
        replicas.append(replica)
        setattr(namespace, self.dest, replicas)


class OrderByArgument(argparse.Action):
    @staticmethod
    def format_usage():
        return "[(asc|desc)] <attribute>"

    def __call__(self, parser, namespace, values, option_string):
        def raise_usage():
            raise ClientError(
                "argument order-by usage {}".format(self.format_usage()),
            )

        clauses = get_value(getattr(namespace, self.dest), [])
        if len(values) == 1:
            clauses.append({"expression": values[0]})
        else:
            if len(values) != 2:
                raise_usage()
            descending = False
            if values[0] == "desc":
                descending = True
            elif values[0] == "asc":
                descending = False
            else:
                raise_usage()
            clauses.append({"descending": descending, "expression": values[1]})
        setattr(namespace, self.dest, clauses)


class UniqueStore(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if getattr(namespace, self.dest, self.default) is not self.default:
            parser.error(option_string + " appears several times.")
        setattr(namespace, self.dest, values)
