import sys
from library.python import func


def try_get_resource(name):
    if am_i_binary():
        import run_import_hook
        return run_import_hook.importer.get_data(name)
    else:
        return None


@func.lazy
def am_i_binary():
    return getattr(sys, 'is_standalone_binary', False)
