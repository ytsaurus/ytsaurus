#!/usr/bin/env python

from yamr_import import Importer

def main():
    importer = Importer("aristotle")
    importer.process_log("day", "user_sessions/{}", None, period=90, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("day", "user_sessions/{}/frauds", "user_sessions_frauds/{}", period=45, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("day", "user_sessions/{}/spy_log", "user_sessions_spy_log/{}", period=45, ydf_attribute="UserSessions.ReadRecord")
    importer.process_log("hour", "fast_logs/twitter_firehose/{}", None, 48)
    importer.process_log("hour", "fast_logs/spy_log/{}", None, 48)
    importer.process_log("hour", "fast_logs/facebook_firehose/{}", None, 48)
    importer.process_log("day", "tr/user_sessions/{}", None, 50, ydf_attribute="UserSessions.ReadRecord")

if __name__ == "__main__":
    main()

