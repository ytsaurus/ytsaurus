# coding: utf-8

from . import formatter


class TeamCitySupport(formatter.BaseSupport):

    def format(self, txt):
        return "##teamcity[message text='{}']\n".format(self._escape(txt))

    @staticmethod
    def _escape(text):
        if text:
            for escape, replacement in [("|", "||"), ("'", "|'"), ("[", "|["), ("]", "|]"), ("\n", "|n"), ("\r", "|r")]:
                text = text.replace(escape, replacement)
        return text
