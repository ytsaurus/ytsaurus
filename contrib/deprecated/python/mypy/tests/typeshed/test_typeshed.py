from yatest.common import source_path  # noqa


def test_check_consistent():
    from mypy.typeshed.tests.check_consistent import main_param  # noqa
    main_param(source_path("contrib/deprecated/python/mypy/mypy/typeshed"))


def test_mypy_test():
    from mypy.typeshed.tests.mypy_test import main_param  # noqa

    class Args:
        def __init__(self):
            self.verbose = 0
            self.dry_run = False
            self.new_analyzer = False
            self.exclude = []
            self.filter = []
            self.python_version = ["3.9", "2.7"]  # "3.6", "3.5", "3.4",
            self.warn_unused_ignores = True
            self.platform = None

    main_param(source_path("contrib/deprecated/python/mypy/mypy/typeshed"), source_path("contrib/deprecated/python/mypy/mypy/typeshed/tests"), Args())
