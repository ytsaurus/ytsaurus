# -*- coding: utf-8 -*-

from .spec import get_spec_preset, get_mixin, merge_specs

import argparse

##################################################################


class ExpertHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """
    Extension of argparse.HelpFormatter handling multiple levels of help verbosity.

    If |skip_expert_section| is True, all arguments with names not in |whitelist|
    are skipped. Section names starting with |EXPERT| are also skipped.

    NB: |whitelist| and |skip_expert_section| are class variables. This class
    should not be instantiated.

    We override some methods despite argparse docs say that only the class name of
    HelpFormatter is a part of public API. Well, ¯\_(ツ)_/¯.
    """
    skip_expert_section = False
    whitelist = set()

    EXPERT = "--EXPERT--"

    def __init__(self, *args, **kwargs):
        kwargs["max_help_position"] = 30
        super(ExpertHelpFormatter, self).__init__(*args, **kwargs)

    def add_usage(self, usage, actions, groups, prefix=None):
        if self.skip_expert_section:
            actions = [action for action in actions if action.dest in self.whitelist]
        return super(ExpertHelpFormatter, self).add_usage(usage, actions, groups, prefix)
        return super(ExpertHelpFormatter, self).__init__()

    def add_argument(self, action):
        if self.skip_expert_section and action.dest not in self.whitelist:
            return
        super(ExpertHelpFormatter, self).add_argument(action)

    def add_text(self, text):
        if text and text.startswith(self.EXPERT):
            if self.skip_expert_section:
                return
            text = text.lstrip(self.EXPERT)
        super(ExpertHelpFormatter, self).add_text(text)


class ArgumentParser(argparse.ArgumentParser):
    """
    Extension of argparse.ArgumentParser handling multilevel help coupled
    with ExpertHelpFormatter.

    Argument |--help| with count action must be added by the user.
    """

    def __init__(self, **kwargs):
        super(ArgumentParser, self).__init__(add_help=False, formatter_class=ExpertHelpFormatter, **kwargs)
        self.namespace = argparse.Namespace()

    def print_help(self, file=None):
        ExpertHelpFormatter.skip_expert_section = self.namespace.help <= 1
        ret = super(ArgumentParser, self).print_help(file=file)
        if self.namespace.help <= 1:
            print()
            print("Use -hh for detailed help.")
        return ret

    def print_usage(self, file=None):
        return super(ArgumentParser, self).print_usage(file=file)

    def error(self, message):
        if self.namespace.help > 0:
            self.print_help()
            exit(0)
        return super(ArgumentParser, self).error(message)

    def parse_args(self):
        return super(ArgumentParser, self).parse_args(namespace=self.namespace)

##################################################################


class SpecBuilder(object):
    """
    Interop between argparse and spec. Provides several helpers to define arguments
    which then automatically convert to spec parameters.
    """
    def __init__(self, parser):
        self.options = {}
        self.maybe_unrecognized = set()
        self.mixins = {}
        self.mutually_exclusive_groups = []
        self.parser = parser
        self.parsers_stack = [parser]
        self.path = []
        self.current_mutex_group = None

    def group(self, group_name, subtree_key=None, **kwargs):
        """Create argument group with name |group_name| descending to |subtree_key| in spec."""
        return GroupSubparserContext(self, group_name, subtree_key, **kwargs)

    def mutex(self, **kwargs):
        """Create mutually exclusive argument group."""
        return MutexSubparserContext(self, **kwargs)

    def add_argument(self, name, path=None, allow_unrecognized=False, **kwargs):
        """Add a simple argument."""
        if kwargs.get("action", None) in ("store_true", "store_false"):
            kwargs["default"] = None
        self.parser.add_argument("--" + name.replace("_", "-"), **kwargs)
        self._store_argument(name, path, allow_unrecognized)

    def add_yesno_argument(self, name, path=None, **kwargs):
        """Add two arguments --name and --no-name."""
        args_without_help = dict(kwargs)
        args_without_help.pop("help", None)

        group = self.parser.add_mutually_exclusive_group()
        group.add_argument("--" + name.replace("_", "-"), default=None, action="store_true", **args_without_help)
        group.add_argument("--no-" + name.replace("_", "-"), default=None, action="store_false", dest=name, **kwargs)
        self._store_argument(name, path)

    def add_enable_disable_argument(self, name, path=None, **kwargs):
        """Add two arguments --enable-name and --disable-name, stored to |enable_name|."""
        args_without_help = dict(kwargs)
        args_without_help.pop("help", None)

        group = self.parser.add_mutually_exclusive_group()
        group.add_argument("--enable-" + name.replace("_", "-"), default=None, action="store_true", dest=name, **args_without_help)
        group.add_argument("--disable-" + name.replace("_", "-"), default=None, action="store_false", dest=name, **kwargs)
        self._store_argument(name, path)

    def add_bool_mixin_argument(self, name, mixin_name=None, **kwargs):
        """Add a nullary mixin (predefined values of several arguments under a short name)."""
        self.parser.add_argument("--" + name.replace("_", "-"), action="store_true", default=None, **kwargs)
        self.mixins[name] = (mixin_name or name, False)

    def add_typed_mixin_argument(self, name, mixin_name=None, no_base_argument=False, **kwargs):
        """Add a mixin with argument (predefined values of several arguments under a short name).

        :param no_base_argument: do not create argument |mixin_name|. Useful when there are several
        other arguments that do store_const to |mixin_name|.
        """
        if not no_base_argument:
            self.parser.add_argument("--" + name.replace("_", "-"), **kwargs)
        self.mixins[name] = (mixin_name or name, True)

    def build_spec(self, args):
        """Make a final spec with all arguments merged."""
        self.spec = get_spec_preset(args.preset)

        for attr, (mixin, is_typed) in self.mixins.items():
            if getattr(args, attr):
                print("Merging mixin {}".format(mixin))
                if is_typed:
                    self.spec = merge_specs(self.spec, get_mixin(mixin, getattr(args, attr)))
                else:
                    self.spec = merge_specs(self.spec, get_mixin(mixin))
                delattr(args, attr)

        for group in self.mutually_exclusive_groups:
            for element in group:
                if getattr(args, element) is not None:
                    for other in group:
                        if other != element:
                            self._merge(None, self.options[other], allow_none=True)

        for option, path in self.options.items():
            self._merge(getattr(args, option), path, allow_unrecognized=option in self.maybe_unrecognized)
            delattr(args, option)
        return self.spec

    def _merge(self, option, path, allow_none=False, allow_unrecognized=False):
        if allow_none or option is not None:
            print("Merging {} to {}".format(option, path))
            path = path.split("/")
            root = {}
            vertex = root
            for key in path[:-1]:
                vertex[key] = {}
                vertex = vertex[key]
            vertex[path[-1]] = option
            self.spec = merge_specs(self.spec, root, allow_unrecognized=allow_unrecognized)

    def _store_argument(self, name, path, allow_unrecognized=False):
        if path is None:
            path = name
        if path.startswith("/"):
            path = path[1:]
        else:
            path = "/".join(self.path + [path])
        self.options[name] = path
        if allow_unrecognized:
            self.maybe_unrecognized.add(name)
        if self.current_mutex_group is not None:
            self.current_mutex_group.add(name)

    def _enter_group(self, parser, subtree_key):
        self.parsers_stack.append(parser)
        self.parser = parser
        if subtree_key:
            self.path.append(subtree_key)

    def _exit_group(self, should_pop_subtree):
        self.parsers_stack.pop(-1)
        self.parser = self.parsers_stack[-1]
        if should_pop_subtree:
            self.path.pop(-1)


class BaseSubparserContext(object):
    """
    Helpers for SpecBuilder.group() and SpecBuilder.mutex().

    The author is too lazy and the code is too short, so please read the code.
    """
    def __init__(self, owner, subtree_key=None):
        self.owner = owner
        self.parser = None
        self.subtree_key = subtree_key

    def __enter__(self):
        self.owner._enter_group(self.parser, self.subtree_key)

    def __exit__(self, exc_type, exc_value, traceback):
        self.owner._exit_group(self.subtree_key is not None)


class GroupSubparserContext(BaseSubparserContext):
    def __init__(self, owner, group_name, subtree_key, **kwargs):
        super(GroupSubparserContext, self).__init__(owner, subtree_key)
        self.parser = self.owner.parser.add_argument_group(group_name, **kwargs)


class MutexSubparserContext(BaseSubparserContext):
    def __init__(self, owner, **kwargs):
        super(MutexSubparserContext, self).__init__(owner)
        self.parser = self.owner.parser.add_mutually_exclusive_group(**kwargs)

    def __enter__(self):
        super(MutexSubparserContext, self).__enter__()
        assert self.owner.current_mutex_group is None
        self.owner.current_mutex_group = set()

    def __exit__(self, exc_type, exc_value, traceback):
        super(MutexSubparserContext, self).__exit__(exc_type, exc_value, traceback)
        assert self.owner.current_mutex_group is not None
        self.owner.mutually_exclusive_groups.append(self.owner.current_mutex_group)
        self.owner.current_mutex_group = None
