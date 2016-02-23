#!/usr/bin/env python

import yt.yson as yson
from yamr_import import Importer

def main():
    user_session_args = {
        #"format_attribute": yson.loads("<key_column_names=[key];escape_carriage_return=true;has_subkey=true;subkey_column_names=[subkey]>yamred_dsv"),
        "ydf_attribute": "UserSessions.ReadRecord"}

    importer = Importer("plato")
    importer.process_log("day", "user_sessions/{}", None, period=220, **user_session_args)
    importer.process_log("day", "user_sessions/{}/frauds", "user_sessions_frauds/{}", period=270, **user_session_args) 
    importer.process_log("day", "user_sessions/{}/spy_log", "user_sessions_spy_log/{}", period=180, **user_session_args)
    importer.process_log("day", "user_intents/{}", None, period=None)
    importer.process_log("day", "reqregscdata/{}/www", None, period=180, link=False)
    importer.process_log("day", "reqregscdata/{}/xml", None, period=180, link=False)
    importer.process_log("day", "reqregscdata/{}/www/fraud", "reqregscdata/{}/www_fraud", period=180, link=False)
    importer.process_log("hour", "fast_logs/user_sessions/{}", None, 48, **user_session_args)

if __name__ == "__main__":
    main()
