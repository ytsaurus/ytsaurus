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

def main():
    bak_dir = "//home/ermolovd/yt-tutorial.bak"
    tutorial_dir = "//home/ermolovd/yt-tutorial"

    def tutorial_table(table_name):
        return tutorial_dir + "/" + table_name

    yt.wrapper.config.set_proxy("freud")
    with yt.wrapper.Transaction():
        if yt.wrapper.exists(tutorial_dir):
            if yt.wrapper.exists(bak_dir):
                yt.wrapper.remove(bak_dir, recursive=True)
            yt.wrapper.move(tutorial_dir, bak_dir)
        yt.wrapper.create("map_node", tutorial_dir)

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

        yt.wrapper.write_table(tutorial_table("staff_unsorted"), staff_unsorted())
        yt.wrapper.write_table(tutorial_table("is_robot_unsorted"), is_robot_unsorted())

    print "https://yt.yandex-team.ru/freud/#page=navigation&path={path}".format(path=tutorial_dir)

if __name__ == "__main__":
    main()
