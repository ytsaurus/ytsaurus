# Tiny registry so the entry point prints ONE coherent load banner, instead of
# each module emitting its own ad-hoc line. Underscore-prefixed, so the package
# auto-loader skips it; command modules import it explicitly.

_commands = []   # command names, in load order
_notes = []      # free-form one-line notes (e.g. the pretty-printer summary)


def command(*names):
    """Register command name(s) for the load banner."""
    _commands.extend(names)


def note(text):
    """Register a free-form one-line note for the load banner."""
    _notes.append(text)


def print_banner():
    """Print the collected commands (grouped by yt-<family>- prefix) and notes."""
    if _commands:
        order, groups = [], {}
        for name in _commands:
            parts = name.split("-")
            family = "-".join(parts[:2]) if len(parts) > 2 else name
            if family not in groups:
                groups[family] = []
                order.append(family)
            groups[family].append(name)
        families = [", ".join(groups[family]) for family in order]
        print("[yt-gdb] commands: " + "  |  ".join(families))
    for text in _notes:
        print("[yt-gdb] " + text)
