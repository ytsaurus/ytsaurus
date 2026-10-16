from yt.common import YtError

import contextlib
import re


@contextlib.contextmanager
def raises_yt_error(message_pattern=None, code=None, required=True):
    """
    Context manager that helps to check that code raises YTError.

    ``message_pattern`` (the first positional argument) is a string matched
    against the raised error: ``.*``, ``.+`` and ``|`` inside it are treated as
    regex wildcards/alternation, all other characters match literally.
    Alternation is useful when different cluster versions report the same failure
    with slightly different wording.

    ``code`` is an integer error code; if set, the raised error must contain it.

    Both checks may be combined. With neither set, any YTError is accepted.
    Value of context manager is a single-element list containing caught error.

    Examples:
        with raises_yt_error(code=yt_error_codes.SortOrderViolation):
            ...

        with raises_yt_error("Name of struct field #0 is empty"):
            ...

        with raises_yt_error("is over .* limit"):
            ...

        with raises_yt_error("cannot be greater than|should not be greater than"):
            ...

        with raises_yt_error("Quota exceeded", code=yt_error_codes.AccountLimitExceeded):
            ...

        with raises_yt_error() as err:
            ...
        assert err[0].contains_code(42) and len(err[0].inner_errors) > 0
    """

    result_list = []
    if not isinstance(message_pattern, (str, type(None))):
        raise TypeError("message_pattern must be str or None, actual type: {}".format(message_pattern.__class__))
    if not isinstance(code, (int, type(None))):
        raise TypeError("code must be int or None, actual type: {}".format(code.__class__))
    try:
        yield result_list
        if required:
            raise AssertionError("Expected exception to be raised")
    except YtError as e:
        if code is not None:
            if not e.contains_code(code):
                raise AssertionError(
                    "Raised error doesn't contain error code {}:\n{}".format(
                        code,
                        e,
                    )
                )
        if message_pattern is not None:
            pattern = re.escape(message_pattern).replace(r"\.\*", ".*").replace(r"\.\+", ".+").replace(r"\|", "|")
            # Treat \(...\) as a regex group only when it encloses alternation
            # (i.e. contains an unescaped |); otherwise keep parens literal.
            pattern = re.sub(r"\\\(([^()]*\|[^()]*)\\\)", r"(\1)", pattern)
            if not re.search(pattern, str(e), re.DOTALL):
                raise AssertionError(
                    "Raised error doesn't match \"{}\":\n{}".format(
                        message_pattern,
                        e,
                    )
                )
        result_list.append(e)


@contextlib.contextmanager
def retry_yt_error(codes=[]):
    """
    Context manager that helps to retry yt errors with the given codes.
    If error raised doesn't contain one of the given codes it will be raised again.

    Examples:
        with retry_yt_error(codes=[yt_error_codes.SyncReplicaNotInSync]):
            ...
    """
    while True:
        try:
            yield
            return
        except YtError as e:
            if not any(e.contains_code(code) for code in codes):
                raise e


def assert_yt_error(error, *args, **kwargs):
    with raises_yt_error(*args, **kwargs):
        raise error
