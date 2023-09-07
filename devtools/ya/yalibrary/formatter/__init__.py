# codint: utf-8

from yalibrary.formatter.formatter import Formatter, DryFormatter, truncate, truncate_middle, transform, format_size  # noqa
from yalibrary.formatter.html import HtmlSupport  # noqa
from yalibrary.formatter.plaintext import PlainSupport  # noqa
from yalibrary.formatter.term import TermSupport, ansi_codes_to_markup  # noqa
from yalibrary.formatter.teamcity import TeamCitySupport   # noqa
from yalibrary.formatter.palette import Highlight, Colors   # noqa


def new_formatter(
        is_tty=False,
        profile=None,
        teamcity=False,
        show_status=True
):
    if teamcity:
        support = TeamCitySupport()

    elif is_tty:
        support = TermSupport(profile=profile)
    else:
        show_status = False
        support = PlainSupport()

    return Formatter(support, show_status)
