#include "logical_type_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema ColumnFromYson(const TString& yson)
{
    TColumnSchema column;
    Deserialize(column, ConvertToNode(TYsonString(yson)));
    return column;
}

TEST(TTableSchemaTest, ColumnTypeV1Deserialization)
{
    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=int64;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)));
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=uint64;"
            "  required=%true"
            "}");
        EXPECT_EQ(*column.LogicalType(), *SimpleLogicalType(ESimpleLogicalValueType::Uint64));
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=null;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *SimpleLogicalType(ESimpleLogicalValueType::Null));
        EXPECT_EQ(column.Required(), false);
    }

    EXPECT_ANY_THROW(ColumnFromYson(
        "{"
        " name=x;"
        " type=null;"
        " required=%true;"
        "}"));
}

TEST(TTableSchemaTest, ColumnTypeV2Deserialization)
{
    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=list;"
            "    element=utf8"
            "  }"
            "}");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=list;"
            "    element=utf8"
            "  };"
            "  required=%true;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=list;"
            "    element=utf8"
            "  };"
            "  type=any;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=optional;"
            "    element={"
            "      metatype=optional;"
            "      element=utf8;"
            "    }"
            "  };"
            "  type=any;"
            "  required=%false;"
            "}");
        EXPECT_EQ(
            *column.LogicalType(),
            *OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))));
    }

    EXPECT_ANY_THROW(
        ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=optional;"
            "    element={"
            "      metatype=optional;"
            "      element=utf8"
            "    }"
            "  };"
            "  required=%true;"
            "}"));

    EXPECT_ANY_THROW(
        ColumnFromYson(
            "{"
            "  name=x;"
            "  type_v2={"
            "    metatype=optional;"
            "    element={"
            "      metatype=optional;"
            "      element=utf8"
            "    }"
            "  };"
            "  type=utf8;"
            "}"));
}

TEST(TTableSchemaTest, ColumnTypeV3Deserialization)
{
    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=list;
                item=utf8;
              }
            }
        )");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=list;
                item=utf8;
              };
              required=%true;
            }
        )");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=list;
                item=utf8;
              };
              type=any;
            }
        )");
        EXPECT_EQ(*column.LogicalType(), *ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)));
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8;
                }
              };
              type=any;
              required=%false;
            }
        )");
        EXPECT_EQ(
            *column.LogicalType(),
            *OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))));
    }

    EXPECT_THROW_WITH_SUBSTRING(
        ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8;
                }
              };
              required=%true;
            }
        )"),
        R"("type_v3" doesn't match "required")"
    );

    EXPECT_THROW_WITH_SUBSTRING(
        ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8
                }
              };
              type=utf8;
            }
        )"),
        R"("type_v3" doesn't match "type")"
    );

    EXPECT_THROW_WITH_SUBSTRING(
        ColumnFromYson(R"(
            {
              name=x;
              type_v3={
                type_name=optional;
                item={
                  type_name=optional;
                  item=utf8;
                }
              };
              type_v2={
                metatype=optional;
                element=utf8;
              }
            }
        )"),
        R"("type_v3" doesn't match "type_v2")"
    );
}

TEST(TTableSchemaTest, ColumnSchemaValidation)
{
    auto expectBad = [] (const auto& schema) {
        EXPECT_THROW(ValidateColumnSchema(schema, true, true), std::exception);
    };

    // Empty names are not ok.
    expectBad(TColumnSchema("", EValueType::String));

    // Names starting from SystemColumnNamePrefix are not ok.
    expectBad(TColumnSchema(SystemColumnNamePrefix + "Name", EValueType::String));

    // Names longer than MaxColumnNameLength are not ok.
    expectBad(TColumnSchema(TString(MaxColumnNameLength + 1, 'z'), EValueType::String));

    // Empty lock names are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetLock(TString("")));

    // Locks on key columns are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetLock(TString("LockName")));

    // Locks longer than MaxColumnLockLength are not ok.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetLock(TString(MaxColumnLockLength + 1, 'z')));

    // Column type should be valid according to the ValidateSchemaValueType function.
    // Non-key columns can't be computed.
    expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetExpression(TString("SomeExpression")));

    // Key columns can't be aggregated.
     expectBad(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetAggregate(TString("sum")));

    ValidateColumnSchema(TColumnSchema("Name", EValueType::String));
    ValidateColumnSchema(TColumnSchema("Name", EValueType::Any));
    ValidateColumnSchema(
        TColumnSchema(TString(256, 'z'), EValueType::String)
            .SetLock(TString(256, 'z')));
    ValidateColumnSchema(
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("SomeExpression")));
    ValidateColumnSchema(
        TColumnSchema("Name", EValueType::String)
            .SetAggregate(TString("sum")));

    // Struct field validation
    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"", SimpleLogicalType(ESimpleLogicalValueType::Int8)}
        })));
    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {TString(257, 'a'), SimpleLogicalType(ESimpleLogicalValueType::Int8)}
        })));

    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"\255", SimpleLogicalType(ESimpleLogicalValueType::Int8)}
        })));

    // Key column cannot be of complex type
    expectBad(
        TColumnSchema("Column", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int8)), ESortOrder::Ascending)
    );
}

TEST(TTableSchemaTest, ValidateTableSchemaTest)
{
    auto expectBad = [] (const auto& schemaString) {
        TTableSchema schema;
        Deserialize(schema, ConvertToNode(TYsonString(schemaString)));

        EXPECT_THROW(ValidateTableSchema(schema, true), std::exception);
    };
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=z}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=y}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=x}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
    expectBad("[{name=x;type=int64;sort_order=ascending;expression=\"uint64(y)\"}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]");
}

TEST(TTableSchemaTest, ColumnSchemaProtobufBackwardCompatibility)
{
    NProto::TColumnSchema columnSchemaProto;
    columnSchemaProto.set_name("foo");
    columnSchemaProto.set_type(static_cast<int>(EValueType::Uint64));

    TColumnSchema columnSchema;
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(*columnSchema.LogicalType(), *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64)));
    EXPECT_EQ(columnSchema.GetPhysicalType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");

    columnSchemaProto.set_simple_logical_type(static_cast<int>(ESimpleLogicalValueType::Uint32));
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(*columnSchema.LogicalType(), *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint32)));
    EXPECT_EQ(columnSchema.GetPhysicalType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");
}

TEST(TTableSchemaTest, TestEqualIgnoringRequiredness)
{
    TTableSchema schema1 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    });

    TTableSchema schema2 = TTableSchema({
        TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });

    TTableSchema schema3 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::String)),
    });

    EXPECT_TRUE(schema1 != schema2);
    EXPECT_TRUE(IsEqualIgnoringRequiredness(schema1, schema2));
    EXPECT_FALSE(IsEqualIgnoringRequiredness(schema1, schema3));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
