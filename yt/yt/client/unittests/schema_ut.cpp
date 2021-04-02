#include "logical_type_shortcuts.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/ytree/convert.h>


namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

static void PrintTo(const TColumnSchema& columnSchema, std::ostream* os)
{
    *os << Format("%v", columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema ColumnFromYson(const TString& yson)
{
    return ConvertTo<TColumnSchema>(TYsonStringBuf(yson));
}

TEST(TTableSchemaTest, ColumnTypeV1Deserialization)
{
    using namespace NLogicalTypeShortcuts;

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=int64;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *Optional(Int64()));
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Uint64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Int64);
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=uint64;"
            "  required=%true"
            "}");
        EXPECT_EQ(*column.LogicalType(), *Uint64());
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Uint64), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Uint64);
        EXPECT_EQ(column.Required(), true);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    {
        auto column = ColumnFromYson(
            "{"
            "  name=x;"
            "  type=null;"
            "}");
        EXPECT_EQ(*column.LogicalType(), *SimpleLogicalType(ESimpleLogicalValueType::Null));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.IsOfV1Type(), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Null), true);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Int64), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Null);
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), false);
    }

    EXPECT_ANY_THROW(ColumnFromYson(
        "{"
        " name=x;"
        " type=null;"
        " required=%true;"
        "}"));
}

TEST(TTableSchemaTest, ColumnTypeV3Deserialization)
{
    using namespace NLogicalTypeShortcuts;
    auto listUtf8Column = ColumnFromYson(R"(
        {
          name=x;
          type_v3={
            type_name=list;
            item=utf8;
          }
        }
    )");
    EXPECT_EQ(*listUtf8Column.LogicalType(), *List(Utf8()));
    EXPECT_EQ(listUtf8Column.Required(), true);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(), false);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(ESimpleLogicalValueType::Utf8), false);
    EXPECT_EQ(listUtf8Column.IsOfV1Type(ESimpleLogicalValueType::Any), false);
    EXPECT_EQ(listUtf8Column.CastToV1Type(), ESimpleLogicalValueType::Any);
    EXPECT_EQ(IsV3Composite(listUtf8Column.LogicalType()), true);

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
        EXPECT_EQ(column, listUtf8Column);
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
        EXPECT_EQ(column, listUtf8Column);
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
        EXPECT_EQ(*column.LogicalType(), *Optional(Optional(Utf8())));
        EXPECT_EQ(column.Required(), false);
        EXPECT_EQ(column.IsOfV1Type(), false);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Utf8), false);
        EXPECT_EQ(column.IsOfV1Type(ESimpleLogicalValueType::Any), false);
        EXPECT_EQ(column.CastToV1Type(), ESimpleLogicalValueType::Any);
        EXPECT_EQ(IsV3Composite(column.LogicalType()), true);
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
}

TEST(TTableSchemaTest, MaxInlineHunkSizeSerialization)
{
    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
            }
        )");
        auto serializedColumn = ConvertToAttributes(column);
        EXPECT_FALSE(serializedColumn->FindYson("max_inline_hunk_size").operator bool());
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
              max_inline_hunk_size=100
            }
        )");
        auto serializedColumn = ConvertToAttributes(column);
        EXPECT_EQ(100, serializedColumn->Get<i64>("max_inline_hunk_size"));
    }
}

TEST(TTableSchemaTest, MaxInlineHunkSizeDeserialization)
{
    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
            }
        )");
        EXPECT_FALSE(column.MaxInlineHunkSize().has_value());
    }

    {
        auto column = ColumnFromYson(R"(
            {
              name=x;
              type=string;
              max_inline_hunk_size=100
            }
        )");
        EXPECT_EQ(column.MaxInlineHunkSize(), 100);
    }
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

    ValidateColumnSchema(
        TColumnSchema("Column", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int8)), ESortOrder::Ascending)
    );

    expectBad(
        TColumnSchema("Column", ListLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))), ESortOrder::Ascending)
    );

    expectBad(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(0)
    );

    expectBad(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(-1)
    );

    expectBad(
        TColumnSchema("Column", EValueType::Int64)
            .SetMaxInlineHunkSize(100)
    );

    expectBad(
        TColumnSchema("Column", EValueType::String, ESortOrder::Ascending)
            .SetMaxInlineHunkSize(100)
    );

    ValidateColumnSchema(
        TColumnSchema("Column", EValueType::String)
            .SetMaxInlineHunkSize(100)
    );

    ValidateColumnSchema(
        TColumnSchema("Column", EValueType::Any)
            .SetMaxInlineHunkSize(100)
    );

    expectBad(
        TColumnSchema("Column", StructLogicalType({
            {"foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"bar", SimpleLogicalType(ESimpleLogicalValueType::String)},
        }), ESortOrder::Ascending)
    );
}

TEST(TTableSchemaTest, ValidateTableSchemaTest)
{
    auto expectBad = [] (const auto& schemaString) {
        TTableSchema schema;
        Deserialize(schema, ConvertToNode(TYsonString(TStringBuf(schemaString))));

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

TEST(TTableSchemaTest, EqualIgnoringRequiredness)
{
    auto schema1 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    });

    auto schema2 = TTableSchema({
        TColumnSchema("foo", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
    });

    auto schema3 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::String)),
    });

    EXPECT_TRUE(schema1 != schema2);
    EXPECT_TRUE(IsEqualIgnoringRequiredness(schema1, schema2));
    EXPECT_FALSE(IsEqualIgnoringRequiredness(schema1, schema3));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
