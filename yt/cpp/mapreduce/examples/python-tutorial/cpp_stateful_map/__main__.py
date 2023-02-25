# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
from yt.python.yt.cpp_wrapper import CppJob


def main():
    client = yt.wrapper.YtClient(proxy="freud")
    output_table = "//tmp/{}-pytutorial-stateful-mapper".format(getpass.getuser())
    # Словарь из CppJob будет доступен в FromNode() как TNode.
    client.run_map(
        CppJob("TFilterMapper", {"pattern": "Arkady", "max_distance": 2}),
        source_table="//home/dev/tutorial/staff_unsorted",
        destination_table=output_table,
    )

    print(("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table)))


if __name__ == "__main__":
    main()
