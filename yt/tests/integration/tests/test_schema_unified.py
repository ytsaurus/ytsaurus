from yt_env_setup import YTEnvSetup, unix_only
from yt_commands import *
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
class TestTypeV3Type(YTEnvSetup):
    @pytest.mark.parametrize("type_yson", UNIFIED_TYPES_YSON)
    def test_type_v3_type(self, type_yson):
        type = yson.loads(type_yson)

        # check we can create without exception
        create("table", "//tmp/table", force=True, attributes={
            "schema": make_schema([{
                "name": "column",
                "type_v3": type,
            }])
        })
