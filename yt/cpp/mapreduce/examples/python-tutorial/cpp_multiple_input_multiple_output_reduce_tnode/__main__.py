# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
from yt.python.yt.cpp_wrapper import CppJob


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    sorted_staff_table = "//tmp/{}-pytutorial-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-is_robot-sorted".format(getpass.getuser())
    human_table = "//tmp/{}-pytutorial-humans".format(getpass.getuser())
    robot_table = "//tmp/{}-pytutorial-robots".format(getpass.getuser())

    client.run_sort(
        source_table="//home/dev/tutorial/staff_unsorted", destination_table=sorted_staff_table, sort_by=["uid"]
    )

    client.run_sort(
        source_table="//home/dev/tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    client.run_reduce(
        CppJob("TSplitHumanRobotsReduce"),
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=[robot_table, human_table],
        reduce_by=["uid"],
    )

    print(("Robot table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(robot_table)))
    print(("Human table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(human_table)))
