# -*- coding: utf-8 -*-

import getpass

import yt.wrapper
from yt.python.yt.cpp_wrapper import CppJob


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    # CppJob можно сочетать с питоновскими джобами.
    # Например, можно было бы указать питоновский маппер, а в редьюсер передать CppJob.
    output_mapper_table = "//tmp/{}-pytutorial-filtered-names-cpp".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-name-stat-cpp".format(getpass.getuser())

    client.run_map_reduce(
        CppJob("TNormalizeNameMapper", "el"),
        CppJob("TCountNamesReducer"),
        source_table="//home/dev/tutorial/staff_unsorted",
        spec={"mapper_output_table_count": 1},
        destination_table=[output_mapper_table, output_table],
        reduce_by=["name"],
    )
    print(("Filtered names table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_mapper_table)))
    print(("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table)))
