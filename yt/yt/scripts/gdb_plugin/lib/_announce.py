# Tiny registry so the entry point prints ONE coherent load banner, instead of
# each module emitting its own ad-hoc line. Underscore-prefixed, so the package
# auto-loader skips it; command modules import it explicitly.

_commands = []   # list of (group_label, [command_name, ...]), in load order
_notes = []      # free-form one-line notes (e.g. the pretty-printer summary)


def command(group, *names):
    """Register command name(s) under a human-readable group label for the banner.
    Repeated calls with the same label merge into one group."""
    _commands.append((group, list(names)))


def note(text):
    """Register a free-form one-line note for the load banner."""
    _notes.append(text)


def print_banner():
    """Print the registered commands, grouped by label, followed by the notes."""
    if _commands:
        order, groups = [], {}
        for group, names in _commands:
            if group not in groups:
                groups[group] = []
                order.append(group)
            groups[group].extend(names)
        width = max(len(group) for group in order) + 1  # room for the ':'
        print("Supported commands:")
        for group in order:
            print("  %-*s  %s" % (width, group + ":", "  ".join(groups[group])))
    for text in _notes:
        print(text)
