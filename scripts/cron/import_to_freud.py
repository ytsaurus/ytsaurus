#!/usr/bin/env python

import yt.yson as yson
from yamr_import import Importer

def main():
    importer = Importer("freud")
    user_session_args = {
        "format_attribute": yson.loads("<key_column_names=[key];escape_carriage_return=true;has_subkey=true;subkey_column_names=[subkey]>yamred_dsv"),
        "ydf_attribute": "UserSessions.ReadRecord"}
    importer.process_log("day", "user_sessions/{}", None, period=7, **user_session_args)

if __name__ == "__main__":
    main()
