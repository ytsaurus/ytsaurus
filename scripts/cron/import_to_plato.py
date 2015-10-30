#!/usr/bin/env python

from yamr_import import Importer

def main():
    importer = Importer("plato")
    importer.process_log("day", "user_sessions/{}", None, period=300, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("day", "user_sessions/{}/frauds", "user_sessions_frauds/{}", period=270, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("day", "user_sessions/{}/spy_log", "user_sessions_spy_log/{}", period=180, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("day", "user_intents/{}", None, period=None)
    importer.process_log("day", "reqregscdata/{}/www", None, period=180, link=False)
    importer.process_log("day", "reqregscdata/{}/xml", None, period=180, link=False)
    importer.process_log("day", "reqregscdata/{}/www/fraud", "reqregscdata/{}/www_fraud", period=180, link=False)
    importer.process_log("hour", "fast_logs/user_sessions/{}", None, 48)

if __name__ == "__main__":
    main()
