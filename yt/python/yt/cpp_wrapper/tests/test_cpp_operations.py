from yt.python.yt.cpp_wrapper import CppJob

import yt.wrapper as yt

from yt.common import YtError

from yt.testlib import authors

from .conftest import TEST_DIR

import pytest
import uuid
import copy


SINGLE_VALUE_PATH = TEST_DIR + "/single_value"
SINGLE_VALUE_CONTENT = [{"x": i} for i in range(10)]

STAFF_UNSORTED_PATH = TEST_DIR + "/staff_unsorted"
IS_ROBOT_PATH = TEST_DIR + "/is_robot"
STAFF_UNSORTED_CONTENT = [
    {"name": "Ivan", "login": "ivannn", "uid": 2},
    {"name": "R2 D2", "login": "r2d2", "uid": 0},
    {"name": "Arkady", "login": "ar_kady", "uid": 1},
]
IS_ROBOT_CONTENT = [
    {"is_robot": False, "uid": 1},
    {"is_robot": False, "uid": 2},
    {"is_robot": True, "uid": 0},
]


def _prepare_single_value_table(path=None, client=None):
    if path is None:
        path = SINGLE_VALUE_PATH
    if client is None:
        yt.write_table(path, SINGLE_VALUE_CONTENT)
    else:
        client.write_table(path, SINGLE_VALUE_CONTENT)


def _check_incremented(path, add=1, client=None):
    expected = list(map(lambda row: {"x": row["x"] + add}, SINGLE_VALUE_CONTENT))
    if client is None:
        rows = list(yt.read_table(path))
    else:
        rows = list(client.read_table(path))
    assert rows == expected


def _prepare_logins_table():
    yt.write_table(STAFF_UNSORTED_PATH, STAFF_UNSORTED_CONTENT)
    yt.write_table(IS_ROBOT_PATH, IS_ROBOT_CONTENT)


def _check_emails_table(path):
    assert list(yt.read_table(path)) == [
        {"name": "Ivan", "email": "ivannn@yandex-team.ru"},
        {"name": "R2 D2", "email": "r2d2@yandex-team.ru"},
        {"name": "Arkady", "email": "ar_kady@yandex-team.ru"},
    ]


def _get_random_path():
    return TEST_DIR + uuid.uuid4().hex[:8]


@pytest.mark.usefixtures("yt_env")
class TestCppOperations(object):
    def setup(self):
        _prepare_single_value_table()
        _prepare_logins_table()

    @authors("egor-gutrov")
    def test_simple_map_tnode(self):
        output_table = _get_random_path()
        yt.run_map(
            CppJob("TStatelessIncrementingMapper"),
            source_table=SINGLE_VALUE_PATH,
            destination_table=output_table,
        )
        _check_incremented(output_table)

    @authors("egor-gutrov")
    def test_simple_map_protobuf(self):
        output_table = _get_random_path()
        yt.run_map(
            CppJob("TComputeEmailsProtoMapper"),
            source_table=STAFF_UNSORTED_PATH,
            destination_table=output_table,
        )
        _check_emails_table(output_table)

    @authors("egor-gutrov")
    def test_stateful_map_tnode(self):
        output_table = _get_random_path()
        yt.run_map(
            CppJob("TStatefulIncrementingMapper", 2),
            source_table=SINGLE_VALUE_PATH,
            destination_table=output_table,
        )
        _check_incremented(output_table, 2)

        output_table = _get_random_path()
        yt.run_map(
            CppJob("TStatefulIncrementingMapper", None),
            source_table=SINGLE_VALUE_PATH,
            destination_table=output_table,
        )
        _check_incremented(output_table, 0)

        output_table = _get_random_path()
        yt.run_map(
            CppJob("TStatefulIncrementingMapper", {"add": 5}),
            source_table=SINGLE_VALUE_PATH,
            destination_table=output_table,
        )
        _check_incremented(output_table, 5)

        with pytest.raises(RuntimeError):
            output_table = _get_random_path()
            yt.run_map(
                CppJob("TStatefulIncrementingMapper"),
                source_table=SINGLE_VALUE_PATH,
                destination_table=output_table,
            )
            _check_incremented(output_table, 1)

    @authors("egor-gutrov")
    def test_forgotten_from_node(self):
        with pytest.raises(RuntimeError):
            output_table = _get_random_path()
            yt.run_map(
                CppJob("TStatelessIncrementingMapper", 2),
                source_table=SINGLE_VALUE_PATH,
                destination_table=output_table,
            )

    @authors("egor-gutrov")
    def test_forgotten_protobuf_format(self):
        with pytest.raises(RuntimeError):  # TODO(egor-gutrov): wrap it with YtError?
            yt.run_map(
                CppJob("TProtoMapperWithoutPrepareOperation"),
                source_table=STAFF_UNSORTED_PATH,
                destination_table=_get_random_path(),
            )

    @authors("egor-gutrov")
    def test_multiple_input_multiple_output_reduce_tnode(self):
        sorted_staff_table = _get_random_path()
        yt.run_sort(
            source_table=STAFF_UNSORTED_PATH,
            destination_table=sorted_staff_table,
            sort_by=["uid"]
        )
        sorted_is_robot_table = _get_random_path()
        yt.run_sort(
            source_table=IS_ROBOT_PATH,
            destination_table=sorted_is_robot_table,
            sort_by=["uid"],
        )
        robot_table = _get_random_path()
        human_table = _get_random_path()
        yt.run_reduce(
            CppJob("TSplitHumanRobotsReduce"),
            source_table=[sorted_staff_table, sorted_is_robot_table],
            destination_table=[robot_table, human_table],
            reduce_by=["uid"],
        )
        assert list(yt.read_table(robot_table)) == [{"uid": 0, "login": "r2d2"}]
        assert list(yt.read_table(human_table)) == [
            {"name": "Arkady", "login": "ar_kady", "email": "ar_kady@yandex-team.ru"},
            {"name": "Ivan", "login": "ivannn", "email": "ivannn@yandex-team.ru"},
        ]

    @authors("egor-gutrov")
    def test_with_client(self):
        client = yt.YtClient(config=copy.deepcopy(yt.config.config))
        output_table = _get_random_path()
        client.run_map(
            CppJob("TStatelessIncrementingMapper"),
            source_table=SINGLE_VALUE_PATH,
            destination_table=output_table,
        )
        _check_incremented(output_table)

    @authors("egor-gutrov")
    def test_from_transaction(self):
        with yt.Transaction():
            input_table = _get_random_path()
            output_table = _get_random_path()
            _prepare_single_value_table(input_table)

            yt.run_map(
                CppJob("TStatelessIncrementingMapper"),
                source_table=input_table,
                destination_table=output_table,
            )
            _check_incremented(output_table)

        client = yt.YtClient(config=copy.deepcopy(yt.config.config))
        with client.Transaction():
            input_table = _get_random_path()
            output_table = _get_random_path()
            _prepare_single_value_table(input_table, client)

            client.run_map(
                CppJob("TStatelessIncrementingMapper"),
                source_table=input_table,
                destination_table=output_table,
            )
            _check_incremented(output_table, client=client)

    @authors("egor-gutrov")
    def test_fail_on_format_from_python(self):
        with pytest.raises(YtError):
            yt.run_map(
                CppJob("TStatelessIncrementingMapper"),
                source_table=SINGLE_VALUE_PATH,
                destination_table=_get_random_path(),
                input_format="yson",
            )
        with pytest.raises(YtError):
            yt.run_map(
                CppJob("TStatelessIncrementingMapper"),
                source_table=SINGLE_VALUE_PATH,
                destination_table=_get_random_path(),
                output_format="yson",
            )
