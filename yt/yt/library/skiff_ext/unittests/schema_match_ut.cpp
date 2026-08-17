#include "ut_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/skiff/skiff_schema.h>

namespace NYT {
namespace {

using namespace NSkiff;
using namespace NSkiffExt;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaParseTest, TestAllowedTypes)
{
    EXPECT_EQ(
        "{uint64,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("uint64")
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{string32,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("string32")
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{variant8<string32;int64;>,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                    .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item()
                                .BeginMap()
                                    .Item("wire_type")
                                    .Value("string32")
                                .EndMap()
                                .Item()
                                .BeginMap()
                                    .Item("wire_type")
                                    .Value("int64")
                                .EndMap()
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()));

    EXPECT_EQ(
        "{variant8<int64;string32;>,}",

        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("skiff_schema_registry")
                    .BeginMap()
                        .Item("item1")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("int64")
                        .EndMap()
                        .Item("item2")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("string32")
                        .EndMap()
                    .EndMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                    .Item()
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item().Value("$item1")
                                .Item().Value("$item2")
                            .EndList()
                        .EndMap()
                    .EndList()
                .EndMap()));
}

TEST(TSkiffSchemaParseTest, TestRecursiveTypesAreDisallowed)
{
    try {
        ConvertToSkiffSchemaShortDebugString(
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("skiff_schema_registry")
                    .BeginMap()
                        .Item("item1")
                        .BeginMap()
                            .Item("wire_type")
                            .Value("variant8")
                            .Item("children")
                            .BeginList()
                                .Item().Value("$item1")
                            .EndList()
                        .EndMap()
                    .EndMap()
                    .Item("table_skiff_schemas")
                    .BeginList()
                        .Item().Value("$item1")
                    .EndList()
                .EndMap());
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("recursive types are forbidden"));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaDescriptionTest, TestDescriptionDerivation)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("Bar"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    EXPECT_EQ(std::ssize(tableDescriptionList), 1);
    EXPECT_EQ(tableDescriptionList[0].HasOtherColumns, false);
    EXPECT_EQ(tableDescriptionList[0].SparseFieldDescriptionList.empty(), true);

    auto denseFieldDescriptionList = tableDescriptionList[0].DenseFieldDescriptionList;
    EXPECT_EQ(std::ssize(denseFieldDescriptionList), 2);

    EXPECT_EQ(denseFieldDescriptionList[0].Name(), "Foo");
    EXPECT_EQ(denseFieldDescriptionList[0].ValidatedGetDeoptionalizeType(/*simplify*/ true), EWireType::Uint64);
}

TEST(TSkiffSchemaDescriptionTest, TestKeySwitchColumn)
{
    {
        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        });

        auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        EXPECT_EQ(std::ssize(tableDescriptionList), 1);
        EXPECT_EQ(tableDescriptionList[0].KeySwitchFieldIndex, std::optional<size_t>(1));
    }
    {
        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$key_switch"),
        });

        try {
            auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
            ADD_FAILURE();
        } catch (const std::exception& e) {
            EXPECT_THAT(e.what(), testing::HasSubstr("Column \"$key_switch\" has unexpected Skiff type"));
        }
    }
}

TEST(TSkiffSchemaDescriptionTest, TestDisallowEmptyNames)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Int64)->SetName(""),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("must have a name"));
    }
}

TEST(TSkiffSchemaDescriptionTest, TestWrongRowType)
{
    auto schema = CreateRepeatedVariant16Schema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid wire type for table row"));
    }
}

TEST(TSkiffSchemaDescriptionTest, TestOtherColumnsOk)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    ASSERT_EQ(std::ssize(tableDescriptionList), 1);
    ASSERT_EQ(tableDescriptionList[0].HasOtherColumns, true);
}

TEST(TSkiffSchemaDescriptionTest, TestOtherColumnsWrongType)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$other_columns"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid wire type for column \"$other_columns\""));
    }
}

TEST(TSkiffSchemaDescriptionTest, TestOtherColumnsWrongPlace)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("$other_columns"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
    });

    try {
        CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Invalid placement of special column \"$other_columns\""));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
