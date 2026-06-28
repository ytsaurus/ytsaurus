# Tiny registry so the entry point prints ONE coherent load banner, instead of
# each module emitting its own ad-hoc line. Underscore-prefixed, so the package
# auto-loader skips it; command modules import it explicitly.

_commands = []   # list of (group_label, [command_name, ...]), in load order
_notes = []      # free-form one-line notes (e.g. the pretty-printer summary)

# Preferred display order for the well-known groups -- load order is otherwise an
# artifact of import side effects (e.g. tcmalloc registers early, as a transitive
# import of type_info). Groups not listed keep their load order, after these.
_GROUP_ORDER = ("ref-counted", "fibers", "printing", "tcmalloc")


def command(group, *names):
    """Register command name(s) under a human-readable group label for the banner.
    Repeated calls with the same label merge into one group."""
    _commands.append((group, list(names)))


def note(text):
    """Register a free-form one-line note for the load banner."""
    _notes.append(text)


def print_banner():
    """Print the registered commands, grouped by label, followed by the notes.

    Each group lists its commands comma-separated; each note is rendered as a
    "Label:" header with its body indented underneath, matching the commands
    block, and set off by a blank line."""
    if _commands:
        order, groups = [], {}
        for group, names in _commands:
            if group not in groups:
                groups[group] = []
                order.append(group)
            groups[group].extend(names)
        # Stable sort: known groups first in their preferred order, the rest after.
        order.sort(key=lambda g: _GROUP_ORDER.index(g) if g in _GROUP_ORDER else len(_GROUP_ORDER))
        width = max(len(group) for group in order) + 1  # room for the ':'
        print("")
        print("Supported commands:")
        for group in order:
            print("  %-*s  %s" % (width, group + ":", ", ".join(groups[group])))
    for text in _notes:
        print("")
        head, sep, rest = text.partition(": ")
        if sep:
            print("%s:" % head)
            print("  %s" % rest)
        else:
            print(text)
