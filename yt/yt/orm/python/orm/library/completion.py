import argcomplete
import sys


def parse_args(cli_name, parser, subparsers):
    _add_print_shell_completion_hook_parser(subparsers)

    argcomplete.autocomplete(parser, append_space=False, default_completer=argcomplete.SuppressCompleter())

    args = parser.parse_args()

    if getattr(args, "func", None) == _print_shell_hook:
        _print_shell_hook(cli_name, args.prefix, args.command, args.shell)
        sys.exit(0)

    return args


def _add_print_shell_completion_hook_parser(subparsers):
    parser = subparsers.add_parser(
        "print-shell-completion-hook",
        help="Print completion hook for the shell",
        description=(
            "For bash-compatible shells output of the command shall be saved to "
            "/etc/bash_completion.d/<some file name>"
        ),
    )
    parser.set_defaults(func=_print_shell_hook)
    parser.add_argument(
        "--prefix",
        type=str,
        required=True,
        help="Shell command prefix to register completion for.",
    )
    parser.add_argument(
        "--command",
        type=str,
        required=True,
        help="How to invoke current CLI. Must be a real binary and not a shell alias.",
    )
    parser.add_argument(
        "--shell",
        type=str,
        choices=("bash", "zsh"),
        required=True,
    )


def _print_shell_hook(cli_name, prefix, command, shell):
    template = _SHELL_HOOKS[shell]
    hook = template.replace("%cli_name%", cli_name).replace("%prefix%", prefix).replace("%command%", command)
    print(hook)


_BASH_HOOK = """\
_invoke_%cli_name%_completion() {
    if [[ -z "${_ARC_DEBUG-}" ]]; then
        %command% 8>&1 9>&2 1>/dev/null 2>&1 </dev/null
    else
        %command% 8>&1 9>&2 1>&9 2>&1 </dev/null
    fi
}

_complete_%cli_name%() {
    local IFS=$'\\013'

    local SUPPRESS_SPACE=0
    if compopt +o nospace 2> /dev/null; then
        SUPPRESS_SPACE=1
    fi

    COMPREPLY=($(IFS="$IFS" \\
        COMP_LINE="$COMP_LINE" \\
        COMP_POINT="$COMP_POINT" \\
        COMP_TYPE="$COMP_TYPE" \\
        _ARGCOMPLETE_COMP_WORDBREAKS="$COMP_WORDBREAKS" \\
        _ARGCOMPLETE=1 \\
        _ARGCOMPLETE_SHELL="bash" \\
        _ARGCOMPLETE_SUPPRESS_SPACE=$SUPPRESS_SPACE \\
        _invoke_%cli_name%_completion))

    if [[ $? != 0 ]]; then
        unset COMPREPLY

    elif [[ $SUPPRESS_SPACE == 1 ]] && [[ "${COMPREPLY-}" =~ [=/:]$ ]]; then
        compopt -o nospace
    fi
}

complete -o nospace -o default -o bashdefault -F _complete_%cli_name% %prefix%
"""

_ZSH_HOOK = """\
#compdef %prefix%

_invoke_%cli_name%_completion() {
    if [[ -z "${_ARC_DEBUG-}" ]]; then
        %command% 8>&1 9>&2 1>/dev/null 2>&1 </dev/null
    else
        %command% 8>&1 9>&2 1>&9 2>&1 </dev/null
    fi
}

_complete_%cli_name%() {
    local IFS=$'\\013'

    local completions=($(IFS="$IFS" \\
        COMP_LINE="$BUFFER" \\
        COMP_POINT="$CURSOR" \\
        _ARGCOMPLETE=1 \\
        _ARGCOMPLETE_SHELL="zsh" \\
        _ARGCOMPLETE_SUPPRESS_SPACE=1 \\
        _invoke_%cli_name%_completion))

    local nosort=()
    if is-at-least 5.8; then
        nosort=(-o nosort)
    fi

    _describe "%cli_name%" completions "${nosort[@]}" -S ''
}

autoload is-at-least

if [[ $zsh_eval_context == *func ]]; then
    _complete_%cli_name% "$@"
else
    compdef _complete_%cli_name% %prefix%
fi
"""

_SHELL_HOOKS = {
    "bash": _BASH_HOOK,
    "zsh": _ZSH_HOOK,
}
