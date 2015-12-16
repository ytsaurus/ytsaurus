#!/usr/bin/env python

from yamr_import import Importer

def main():
    importer = Importer("freud")
    importer.process_log("day", "user_sessions/{}", None, period=7, ydf_attribute="UserSessions.ReadRecord")

if __name__ == "__main__":
    main()
