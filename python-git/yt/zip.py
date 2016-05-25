import sys

if sys.version_info[:2] == (2, 6):
    import zipfile
    import gzip

    class ZipFile(zipfile.ZipFile):
        def __init__(self, file, mode="r", compression=zipfile.ZIP_STORED, allowZip64=False):
            zipfile.ZipFile.__init__(self, file, mode, compression, allowZip64)

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.close()

    class GzipFile(gzip.GzipFile):
        def __init__(self, *args, **kwargs):
            gzip.GzipFile.__init__(self, *args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            self.close()
else:
    from zipfile import ZipFile
    from gzip import GzipFile
