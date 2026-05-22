from yt_env_setup import YTEnvSetup

from yt.environment.helpers import assert_items_equal

from yt_commands import authors, create, write_table, read_table, alter_table

from yt_type_helpers import make_schema

import yt.yson as yson

import json

import pytest

# Test examples from
# https://a.yandex-team.ru/arc/trunk/arcadia/logfeller/mvp/docs/types_serialization.md

UNIFIED_TYPES_YSON = [
    """
    {
       type_name=int16;
    }
    """,
    """
    utf8
    """,
    """
    {
        type_name=optional;
        item=string;
    }
    """,
    """
    {
       type_name=list;
       item={
         type_name=list;
         item=bool;
       }
    }
    """,
    """
    {
       type_name=optional;
       item={
         type_name=optional;
         item=bool;
       }
    }
    """,
    """
    {
      type_name=struct;
      members=[
        {
          name=foo;
          type=bool;
        };
        {
          name=bar;
          type={
            type_name=struct;
            members=[
                {
                  name=foo;
                  type=bool;
                };
            ];
          }
        };
      ]
    }
    """,
    """
    {
      type_name=tuple;
      elements=[
        {
          type=double;
        };
        {
          type=double;
        };
      ]
    }
    """,
    """
    {
      type_name=variant;
      members=[
        {
           name=int_field;
           type=int64;
        };
        {
           name=string_field;
           type=string;
        };
      ]
    }
    """,
    """
    {
      type_name=variant;
      elements=[
          {
             type=null;
          };
          {
             type=string;
          };
      ]
    }
    """,
    """
    {
      type_name=dict;
      key=string;
      value={
        type_name=optional;
        item=int32;
      }
    }
    """,
    """
    {
      type_name=tagged;
      tag="image/svg";
      item="string";
    }
    """,
]

UNIFIED_TYPES_YSON = [s.strip() for s in UNIFIED_TYPES_YSON]


@authors("ermolovd")
@pytest.mark.enabled_multidaemon
class TestTypeV3Type(YTEnvSetup):
    ENABLE_MULTIDAEMON = True

    @pytest.mark.parametrize("type_yson", UNIFIED_TYPES_YSON)
    def test_type_v3_type(self, type_yson):
        type = yson.loads(type_yson.encode("ascii"))

        # check we can create without exception
        create(
            "table",
            "//tmp/table",
            force=True,
            attributes={
                "schema": make_schema(
                    [
                        {
                            "name": "column",
                            "type_v3": type,
                        }
                    ]
                )
            },
        )

    @authors("a-romanov")
    def test_read_web_json_yql_afrer_alter_table_add_leaf(self):
        schema_old = [
            {
                "name": "heap",
                "type": "any",
                "required": True,
                "type_v3": {
                    "type_name": "list",
                    "item": {
                        "type_name": "struct",
                        "members": [
                            {
                                "type": {
                                    "type_name": "optional",
                                    "item": "utf8",
                                },
                                "name": "thought",
                            }
                        ]
                    }
                }
            }
        ]
        schema_new = [
            {
                "name": "heap",
                "type": "any",
                "required": True,
                "type_v3": {
                    "type_name": "list",
                    "item": {
                        "type_name": "struct",
                        "members": [
                            {
                                "type": {
                                    "type_name": "optional",
                                    "item": "utf8",
                                },
                                "name": "thought",
                            },
                            {
                                "type": {
                                    "type_name": "optional",
                                    "item": "utf8",
                                },
                                "name": "added",
                            }
                        ]
                    }
                }
            }
        ]
        rows = [{"heap": [{"thought": "one"}, {"thought": "two"}]}]
        create("table", "//tmp/t", attributes={"schema": schema_old})
        write_table("//tmp/t", rows)
        alter_table("//tmp/t", schema=schema_new)

        res = read_table("//tmp/t")
        expected = [{"heap": [{"thought": "one", "added": None}, {"thought": "two", "added": None}]}]
        assert_items_equal(res, expected)

        expected = {
            'rows': [{'heap': [{'thought': {'$type': 'string', '$value': 'one'}, 'added': None}, {'thought': {'$type': 'string', '$value': 'two'}, 'added': None}]}],
            'incomplete_columns': 'false',
            'incomplete_all_column_names': 'false',
            'all_column_names': ['heap']
        }
        web = json.loads(read_table("//tmp/t", output_format=b'web_json').decode())
        assert_items_equal(web, expected)

        yql = json.loads(read_table("//tmp/t", output_format=yson.loads(b'<value_format=yql>web_json')).decode())
        expected = {
            'rows': [{'heap': [{'val': [[['one'], None], [['two'], None]]}, '8']}],
            'incomplete_columns': 'false',
            'incomplete_all_column_names': 'false',
            'all_column_names': ['heap'],
            'yql_type_registry': [
                ['NullType'],
                ['DataType', 'Int64'],
                ['DataType', 'Uint64'],
                ['DataType', 'Double'],
                ['DataType', 'Boolean'],
                ['DataType', 'String'],
                ['DataType', 'Yson'],
                ['DataType', 'Yson'],
                ['ListType', ['StructType', [['thought', ['OptionalType', ['DataType', 'Utf8']]], ['added', ['OptionalType', ['DataType', 'Utf8']]]]]]
            ]
        }
        assert_items_equal(yql, expected)
