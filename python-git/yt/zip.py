import sys

if sys.version_info[:2] == (2, 6):
    import gzip

    class GzipFile(gzip.GzipFile):
        def __init__(self, *args, **kwargs):
            gzip.GzipFile.__init__(self, *args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.close()
else:
    from gzip import GzipFile
