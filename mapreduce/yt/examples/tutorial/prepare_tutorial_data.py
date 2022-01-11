#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import gzip
import json
import yt.wrapper

# Данные можно взять отсюда:
# https://staff-api.yandex-team.ru/v3/persons?_limit=30000

class Staff(object):
    def __init__(self):
        with gzip.GzipFile("staff.gz", "r") as inf:
            staff = json.loads(inf.read())
        self.staff = staff["result"]

    def iter_staff(self):
        Person = collections.namedtuple("Person", ["name", "login", "uid", "is_robot"])
        for r in self.staff:
            login = r["login"]
            is_robot = r["official"]["is_robot"]
            name = r["name"]["first"]["en"]
            uid = r["uid"]
            yield Person(
                name=name,
                login=login,
                uid=int(uid),
                is_robot=is_robot)

class Urls(object):
    def __init__(self):
        with open("urls.json", "r") as inf:
            data = json.loads(inf.read())

        self.hosts = data["hosts"]
        self.urls = data["urls"]


    def iter_urls(self):
        Document = collections.namedtuple("Document", ["host", "path", "title", "keywords"])
        for d in self.urls:
            yield Document(
                host=d["Host"],
                path=d["Path"],
                title=d["Title"],
                keywords=d["Keywords"],
            )

    def iter_host(self):
        Host = collections.namedtuple("Host", ["host", "video_regexp"])
        for d in self.hosts:
            yield Host(
                host=d["Host"],
                video_regexp=d["VideoRegexp"]
            )

def main():
    bak_dir = "//home/ermolovd/yt-tutorial.bak"
    tutorial_dir = "//home/ermolovd/yt-tutorial"

    def tutorial_table(table_name):
        return tutorial_dir + "/" + table_name

    client = yt.wrapper.YtClient(proxy="freud")
    with yt.wrapper.Transaction():
        if client.exists(tutorial_dir):
            if client.exists(bak_dir):
                client.remove(bak_dir, recursive=True)
            client.move(tutorial_dir, bak_dir)
        client.create("map_node", tutorial_dir)

        def staff_unsorted():
            for p in Staff().iter_staff():
                yield {
                    "name": p.name,
                    "login": p.login,
                    "uid": p.uid,
                }
        def is_robot_unsorted():
            for p in Staff().iter_staff():
                yield {
                    "uid": p.uid,
                    "is_robot": p.is_robot
                }

        def doc_title_table():
            for d in Urls().iter_urls():
                yield {
                    "host": d.host,
                    "path": d.path,
                    "title": d.title,
                }

        def doc_keywords_table():
            for d in Urls().iter_urls():
                yield {
                    "host": d.host,
                    "path": d.path,
                    "keywords": d.keywords,
                }

        def host_table():
            for h in Urls().iter_host():
                yield {
                    "host": h.host,
                    "video_regexp": h.video_regexp,
                }

        client.write_table(yt.wrapper.TablePath(tutorial_table("host_video_regexp"), sorted_by=["host"]),
                               sorted(host_table(), key=lambda x:(x["host"],)))
        client.write_table(yt.wrapper.TablePath(tutorial_table("doc_title"), sorted_by=["host", "path"]),
                               sorted(doc_title_table(), key=lambda x:(x["host"], x["path"])))
        client.write_table(yt.wrapper.TablePath(tutorial_table("doc_keywords"), sorted_by=["host", "path"]),
                                              sorted(doc_keywords_table(), key=lambda x:(x["host"], x["path"])))

        client.write_table(tutorial_table("staff_unsorted"), staff_unsorted())
        client.write_table(tutorial_table("is_robot_unsorted"), is_robot_unsorted())

    print "https://yt.yandex-team.ru/freud/#page=navigation&path={path}".format(path=tutorial_dir)

if __name__ == "__main__":
    main()
