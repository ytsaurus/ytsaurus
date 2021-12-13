# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
from yt.python.yt.cpp_wrapper import CppJob


if __name__ == "__main__":
    yt.wrapper.config.set_proxy("freud")

    sorted_staff_table = "//tmp/" + getpass.getuser() + "-pytutorial-staff-sorted"
    sorted_is_robot_table = "//tmp/" + getpass.getuser() + "-pytutorial-is_robot-sorted"
    human_table = "//tmp/" + getpass.getuser() + "-pytutorial-humans"
    robot_table = "//tmp/" + getpass.getuser() + "-pytutorial-robots"

    yt.wrapper.run_sort(
        source_table="//home/dev/tutorial/staff_unsorted", destination_table=sorted_staff_table, sort_by=["uid"]
    )

    yt.wrapper.run_sort(
        source_table="//home/dev/tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    yt.wrapper.run_reduce(
        CppJob("TSplitHumanRobotsReduce"),
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=[robot_table, human_table],
        reduce_by=["uid"],
    )

    print("Robot table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(robot_table))
    print("Human table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(human_table))
