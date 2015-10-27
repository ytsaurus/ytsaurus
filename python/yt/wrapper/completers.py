from yt.packages.argcomplete import *
import yt.wrapper as yt

if not yt.config["argcomplete_verbose"]:
    warn = lambda *args, **kwargs: None

# The original CompletionFinder performs a fallback to the usual filename completion
# that is unappropriate in most cases for Yt. We inherit from that class in order
# to fix this behaviour.
# TODO(max42): create a pull-request in kislyuk/argcomplete.
OldCompletionFinder = CompletionFinder
class CompletionFinder(OldCompletionFinder):
    def __init__(self, *args, **kwargs):
        self.enable_bash_fallback = kwargs.pop('enable_bash_fallback', True) 
        OldCompletionFinder.__init__(self, *args, **kwargs)

    def _complete_active_option(self, parser, next_positional, cword_prefix, parsed_args, completions):
        debug("Active actions (L={l}): {a}".format(l=len(parser.active_actions), a=parser.active_actions))

        # Only run completers if current word does not start with - (is not an optional)
        if len(cword_prefix) > 0 and cword_prefix[0] in parser.prefix_chars:
            return completions

        for active_action in parser.active_actions:
            if not active_action.option_strings:  # action is a positional
                if action_is_satisfied(active_action) and not action_is_open(active_action):
                    debug("Skipping", active_action)
                    continue

            debug("Activating completion for", active_action, active_action._orig_class)
            # completer = getattr(active_action, "completer", DefaultCompleter())
            completer = getattr(active_action, "completer", None)

            if completer is None and active_action.choices is not None:
                if not isinstance(active_action, argparse._SubParsersAction):
                    completer = completers.ChoicesCompleter(active_action.choices)

            if completer:
                if len(active_action.option_strings) > 0:  # only for optionals
                    if not action_is_satisfied(active_action):
                        # This means the current action will fail to parse if the word under the cursor is not given
                        # to it, so give it exclusive control over completions (flush previous completions)
                        debug("Resetting completions because", active_action, "is unsatisfied")
                        self._display_completions = {}
                        completions = []
                if callable(completer):
                    completions_from_callable = [c for c in completer(
                        prefix=cword_prefix, action=active_action, parsed_args=parsed_args)
                        if self.validator(c, cword_prefix)]

                    if completions_from_callable:
                        completions += completions_from_callable
                        if isinstance(completer, completers.ChoicesCompleter):
                            self._display_completions.update(
                                [[x, active_action.help] for x in completions_from_callable])
                        else:
                            self._display_completions.update(
                                [[x, ""] for x in completions_from_callable])
                else:
                    debug("Completer is not callable, trying the readline completer protocol instead")
                    for i in range(9999):
                        next_completion = completer.complete(cword_prefix, i)
                        if next_completion is None:
                            break
                        if self.validator(next_completion, cword_prefix):
                            self._display_completions.update({next_completion: ""})
                            completions.append(next_completion)
                debug("Completions:", completions)
            elif not isinstance(active_action, argparse._SubParsersAction):
                if self.enable_bash_fallback:
                    debug("Completer not available, falling back")
                    try:
                        # TODO: what happens if completions contain newlines? How do I make compgen use IFS?
                        bashcomp_cmd = ["bash", "-c", "compgen -A file -- '{p}'".format(p=cword_prefix)]
                        comp = subprocess.check_output(bashcomp_cmd).decode(sys_encoding).splitlines()
                        if comp:
                            self._display_completions.update([[x, ""] for x in comp])
                            completions += comp
                    except subprocess.CalledProcessError:
                        pass
                else:
                    debug("Completer not available, doing nothing")                    

        return completions

def complete_ypath(prefix, parsed_args, **kwargs):
    if prefix == "":
        return ["//"]
    try:
        path = yt.TablePath(prefix)
        path = str(path).rsplit("/", 1)[0]
        content = yt.list(path, max_size=10**5, attributes=["type"])
        suggestions = []
        for item in content:
            if item.attributes["type"] == "map_node":
                full_path = path + "/" + str(item)
                if full_path == prefix:
                    suggestions += [full_path + "/"]
                else:
                    suggestions += [full_path, full_path + "/"]
            else:
                suggestions += [full_path]
        return suggestions
    except Exception as e:
        warn("Caught following exception during completion:\n" + str(e))
