#!/usr/bin/python

import re
import sys

PACKAGE_NAME = "yandex-yt-python"
VERSION_PATTERN = re.compile(r"(\d+\.\d+\.\d+-\d+)")
ENDLINE_PATTERN = re.compile(r"^ -- (.*) <(.*)>(  ?)(.*)$")
DATE_PATTERN = re.compile(r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*(\d{2}\s\w+\s\d{4})")

ST_URL = "https://st.yandex-team.ru"
YT_ST_TIKET_PATTERN = re.compile(r"((YT|YTADMIN|YTADMINREQ)-\d+)")

ESCAPE = re.compile(r"[{}*_]")
ESCAPE_DICT = {
    "*": "\\*",
    "{": "\\{",
    "}": "\\}",
    "_": "\\_",
}

def make_heading(text, add_upper_line=True):
    s = "=" * len(text)
    if add_upper_line:
        return "{0}\n{1}\n{0}".format(s, text)
    else:
        return "{0}\n{1}".format(text, s)

def extract_date(line, line_no):
    # Line will have the following format:
    # -- Kolesnichenko Ignat <ignat@yandex-team.ru>  Wed, 12 Sep 2012 22:57:07 +0400
    match = ENDLINE_PATTERN.match(line)
    if not match:
        raise RuntimeError("Failed to parse endline (line: {0})".format(line_no))
    full_date = match.group(4)

    # Full date will be in the following format:
    # Wed, 12 Sep 2012 22:57:07 +0400
    match = DATE_PATTERN.match(full_date)
    if not match:
        raise RuntimeError("Failed to extract date from endline (line: {0})".format(line_no))

    return match.group(2).strip()

def process_st_tickets(line):
    def replace(match):
        ticket = match.group(1).strip()
        return "`{0} <{1}/{0}>`_".format(ticket, ST_URL)

    return YT_ST_TIKET_PATTERN.sub(replace, line)

def escape(line, line_no):
    def replace(match):
        return ESCAPE_DICT[match.group(0)]

    first_star_pos = line.find("*")
    if first_star_pos == -1:
        raise RuntimeError('Incorrect changelog line {0}, line does not contain "*" symbol'
                           .format(line_no))
    return line[:first_star_pos+1] + ESCAPE.sub(replace, line[first_star_pos+1:])

def main():
    with open("debian/changelog") as changelog, open("docs/package_changelog.rst", "w") as changelog_rst:
        changelog_rst.write(make_heading("Changelog") + "\n\n")

        version = None
        version_block = []

        for line_no, line in enumerate(changelog, start=1):
            if not line.strip():
                continue

            if line.startswith(PACKAGE_NAME):
                s = VERSION_PATTERN.search(line)
                if not s:
                    raise RuntimeError("Failed to extract version (line: {0})".format(line_no))
                version = s.group(1)
            elif line.startswith("  *"):
                version_block.append(process_st_tickets(escape(line, line_no)))
            elif line.startswith(" --"):
                date = extract_date(line, line_no)
                heading = "{0} ({1})".format(version, date)
                changelog_rst.write(make_heading(heading, add_upper_line=False) + "\n")
                changelog_rst.writelines(version_block)
                changelog_rst.write("\n")
                version_block = []
            else:
                print >>sys.stderr, "Unexpected line {0}, skipping".format(line_no)

if __name__ == "__main__":
    try:
        main()
    except RuntimeError as err:
        print >>sys.stderr, str(err)
        sys.exit(1)
