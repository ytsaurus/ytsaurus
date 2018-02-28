import sys

if sys.version_info[:2] == (2, 6):
    import tarfile

    class TarFile(tarfile.TarFile):
        def __init__(self, *args, **kwargs):
            tarfile.TarFile.__init__(self, *args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.close()
else:
    from tarfile import TarFile

from tarfile import TarInfo

