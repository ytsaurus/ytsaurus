from yt.wrapper.errors import YtResponseError

import pytest


class TestNestedColumns:
    SPEC = {
        "direct_singular": {"foo": "ds"},
        "direct_repeated": [{"foo": "dr0"}, {"foo": "dr1"}],
        "direct_map": {"dmk": {"foo": "dmv"}},
        "column_singular": {"foo": "cs"},
        "column_repeated": [{"foo": "cr0"}, {"foo": "cr1"}],
        "column_map": {"cmk": {"foo": "cmv"}},
        "composite_singular": {
            "direct_singular": {"foo": "csds"},
            "direct_repeated": [{"foo": "csdr0"}, {"foo": "csdr1"}],
            "direct_map": {"csdmk": {"foo": "csdmv"}},
            "column_singular": {"foo": "cscs"},
            "column_repeated": [{"foo": "cscr0"}, {"foo": "cscr1"}],
            "column_map": {"cscmk": {"foo": "cscmv"}},
        },
        # "composite_repeated": [
        #     {
        #         "direct_singular":{"foo": "cr0ds"},
        #         "direct_repeated": [{"foo":"cr0dr0"},{"foo": "cr0dr1"}],
        #         "direct_map": { "cr0dmk": {"foo": "cr0dmv" }},
        #         "column_singular":{"foo": "cr0cs"},
        #         "column_repeated": [{"foo":"cr0cr0"},{"foo": "cr0cr1"}],
        #         "column_map": { "cr0cmk": {"foo": "cr0cmv" }},
        #     },
        #     {
        #         "direct_singular":{"foo": "cr1ds"},
        #         "direct_repeated": [{"foo":"cr1dr0"},{"foo": "cr1dr1"}],
        #         "direct_map": { "cr1dmk": {"foo": "cr1dmv" }},
        #         "column_singular":{"foo": "cr1cs"},
        #         "column_repeated": [{"foo":"cr1cr0"},{"foo": "cr1cr1"}],
        #         "column_map": { "cr1cmk": {"foo": "cr1cmv" }},
        #     },
        # ]
    }

    def test_direct(self, example_env):
        client = example_env.client
        id = client.create_object(
            "nested_columns", attributes={"spec": TestNestedColumns.SPEC}
        )

        assert (
            "ds"
            == client.get_object("nested_columns", id, ["/spec/direct_singular/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/direct_singular/foo",
                    "value": "dsu",
                }
            ],
        )
        assert (
            "dsu"
            == client.get_object("nested_columns", id, ["/spec/direct_singular/foo"])[0]
        )

        assert (
            "dr0"
            == client.get_object("nested_columns", id, ["/spec/direct_repeated/0/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/direct_repeated/end",
                    "value": {"foo": "dr2"},
                }
            ],
        )
        assert (
            "dr2"
            == client.get_object("nested_columns", id, ["/spec/direct_repeated/2/foo"])[0]
        )

        assert (
            "dmv"
            == client.get_object("nested_columns", id, ["/spec/direct_map/dmk/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/direct_map/dmk2",
                    "value": {"foo": "dmv2"},
                }
            ],
        )
        assert (
            "dmv2"
            == client.get_object("nested_columns", id, ["/spec/direct_map/dmk2/foo"])[0]
        )

    def test_column(self, example_env):
        client = example_env.client
        id = client.create_object(
            "nested_columns", attributes={"spec": TestNestedColumns.SPEC}
        )

        assert (
            "cs"
            == client.get_object("nested_columns", id, ["/spec/column_singular/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/column_singular/foo",
                    "value": "csu",
                }
            ],
        )
        assert (
            "csu"
            == client.get_object("nested_columns", id, ["/spec/column_singular/foo"])[0]
        )

        assert (
            "cr0"
            == client.get_object("nested_columns", id, ["/spec/column_repeated/0/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/column_repeated/end",
                    "value": {"foo": "cr2"},
                }
            ],
        )

        assert (
            "cr2"
            == client.get_object("nested_columns", id, ["/spec/column_repeated/2/foo"])[0]
        )

        assert (
            "cmv"
            == client.get_object("nested_columns", id, ["/spec/column_map/cmk/foo"])[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/column_map/cmk2",
                    "value": {"foo": "cmv2"},
                }
            ],
        )
        assert (
            "cmv2"
            == client.get_object("nested_columns", id, ["/spec/column_map/cmk2/foo"])[0]
        )

    def test_composite_singular_direct(self, example_env):
        client = example_env.client
        id = client.create_object(
            "nested_columns", attributes={"spec": TestNestedColumns.SPEC}
        )

        assert (
            "csds"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_singular/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/direct_singular/foo",
                    "value": "csdsu",
                }
            ],
        )
        assert (
            "csdsu"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_singular/foo"]
            )[0]
        )

        assert (
            "csdr0"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_repeated/0/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/direct_repeated/end",
                    "value": {"foo": "csdr2"},
                }
            ],
        )

        assert (
            "csdr2"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_repeated/2/foo"]
            )[0]
        )

        assert (
            "csdmv"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_map/csdmk/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/direct_map/csdmk2",
                    "value": {"foo": "csdmv2"},
                }
            ],
        )
        assert (
            "csdmv2"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/direct_map/csdmk2/foo"]
            )[0]
        )

    def test_composite_singular_column(self, example_env):
        client = example_env.client
        id = client.create_object(
            "nested_columns", attributes={"spec": TestNestedColumns.SPEC}
        )

        assert (
            "cscs"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_singular/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/column_singular/foo",
                    "value": "cscsu",
                }
            ],
        )
        assert (
            "cscsu"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_singular/foo"]
            )[0]
        )

        assert (
            "cscr0"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_repeated/0/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/column_repeated/end",
                    "value": {"foo": "cscr2"},
                }
            ],
        )
        assert (
            "cscr2"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_repeated/2/foo"]
            )[0]
        )

        assert (
            "cscmv"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_map/cscmk/foo"]
            )[0]
        )
        client.update_object(
            "nested_columns",
            id,
            set_updates=[
                {
                    "path": "/spec/composite_singular/column_map/cscmk2",
                    "value": {"foo": "cscmv2"},
                }
            ],
        )
        assert (
            "cscmv2"
            == client.get_object(
                "nested_columns", id, ["/spec/composite_singular/column_map/cscmk2/foo"]
            )[0]
        )

    def test_deprecated_column(self, example_env):
        schema = example_env.yt_client.get("//home/example/db/nested_columns/@schema")
        column_names = {column["name"] for column in schema}
        assert "spec.deprecated_column_singular" in column_names

    @pytest.mark.parametrize("field_name", ["deprecated_column_singular", "deprecated_direct_singular"])
    def test_deprecated(self, example_env, field_name):
        id = example_env.client.create_object(
            "nested_columns", attributes={"spec": TestNestedColumns.SPEC}
        )
        updates = [dict(path=f"/spec/{field_name}", value=42)]
        with pytest.raises(YtResponseError):
            example_env.client.update_object("nested_columns", id, set_updates=updates)

        updates[0]["path"] = "/spec"
        updates[0]["value"] = {f"{field_name}": 42}
        with pytest.raises(YtResponseError):
            example_env.client.update_object("nested_columns", id, set_updates=updates)

        result = example_env.client.get_object("nested_columns", id, ["/spec"])
        assert f"{field_name}" not in result[0]
