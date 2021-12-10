# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
from yt.python.yt.cpp_wrapper import CppJob


def main():
    yt.wrapper.config.set_proxy("freud")
    output_table = "//tmp/" + getpass.getuser() + "-pytutorial-emails-cpp-tnode"
    yt.wrapper.run_map(
        CppJob("TComputeEmailsMapper"),
        source_table="//home/ermolovd/yt-tutorial/staff_unsorted",
        destination_table=output_table,
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))


if __name__ == "__main__":
    main()
