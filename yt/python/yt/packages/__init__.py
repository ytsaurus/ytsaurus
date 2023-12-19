import os
import sys


def is_arcadia_python():
    try:
        import __res
        assert __res
        return True
    except ImportError:
        pass

    return hasattr(sys, "extra_modules")


if is_arcadia_python():
    class PackagesImporter(object):
        def __enter__(self):
            pass

        def __exit__(self, type, value, traceback):
            pass
else:
    class PackagesImporter(object):
        def __enter__(self):
            self.dir_name = os.path.dirname(__file__)
            sys.path.insert(1, self.dir_name)

        def __exit__(self, type, value, traceback):
            dir_index = -1
            for index, path in enumerate(sys.path):
                if path == self.dir_name:
                    dir_index = index
            if dir_index != -1:
                sys.path.pop(dir_index)
