#include <yt/yt/core/test_framework/framework.h>

#include "row_helpers.h"
#include "logical_type_shortcuts.h"

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/formats/skiff_parser.h>
#include <yt/yt/client/formats/skiff_writer.h>
#include <yt/yt/client/formats/format.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_visitor.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/stream/null.h>
#include <util/string/hex.h>

namespace NYT {

namespace {

using namespace NFormats;
using namespace NSkiff;
using namespace NSkiffExt;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TString ConvertToSkiffSchemaShortDebugString(INodePtr node)
{
    auto skiffFormatConfig = ConvertTo<TSkiffFormatConfigPtr>(std::move(node));
    auto skiffSchemas = ParseSkiffSchemas(skiffFormatConfig->SkiffSchemaRegistry, skiffFormatConfig->TableSkiffSchemas);
    TStringStream result;
    result << '{';
    for (const auto& schema : skiffSchemas) {
        result <<  GetShortDebugString(schema);
        result << ',';
    }
    result << '}';
    return result.Str();
}

////////////////////////////////////////////////////////////////////////////////

TString ConvertToYsonTextStringStable(const INodePtr& node)
{
    TStringStream out;
    TYsonWriter writer(&out, EYsonFormat::Text);
    VisitTree(node, &writer, true, std::nullopt);
    writer.Flush();
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaParse, TestAllowedTypes)
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

TEST(TSkiffSchemaParse, TestRecursiveTypesAreDisallowed)
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

TEST(TSkiffSchemaDescription, TestDescriptionDerivation)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("Bar"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    EXPECT_EQ(tableDescriptionList.size(), 1);
    EXPECT_EQ(tableDescriptionList[0].HasOtherColumns, false);
    EXPECT_EQ(tableDescriptionList[0].SparseFieldDescriptionList.empty(), true);

    auto denseFieldDescriptionList = tableDescriptionList[0].DenseFieldDescriptionList;
    EXPECT_EQ(denseFieldDescriptionList.size(), 2);

    EXPECT_EQ(denseFieldDescriptionList[0].Name(), "Foo");
    EXPECT_EQ(denseFieldDescriptionList[0].ValidatedSimplify(), EWireType::Uint64);
}

TEST(TSkiffSchemaDescription, TestKeySwitchColumn)
{
    {
        auto schema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        });

        auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
        EXPECT_EQ(tableDescriptionList.size(), 1);
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

TEST(TSkiffSchemaDescription, TestDisallowEmptyNames)
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

TEST(TSkiffSchemaDescription, TestWrongRowType)
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

TEST(TSkiffSchemaDescription, TestOtherColumnsOk)
{
    auto schema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Foo"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("Bar"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    auto tableDescriptionList = CreateTableDescriptionList({schema}, RangeIndexColumnName, RowIndexColumnName);
    ASSERT_EQ(tableDescriptionList.size(), 1);
    ASSERT_EQ(tableDescriptionList[0].HasOtherColumns, true);
}

TEST(TSkiffSchemaDescription, TestOtherColumnsWrongType)
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

TEST(TSkiffSchemaDescription, TestOtherColumnsWrongPlace)
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

ISchemalessFormatWriterPtr CreateSkiffWriter(
    std::shared_ptr<TSkiffSchema> skiffSchema,
    TNameTablePtr nameTable,
    IOutputStream* outputStream,
    const std::vector<TTableSchemaPtr>& tableSchemaList,
    int keyColumnCount = 0,
    bool enableEndOfStream = false)
{
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableKeySwitch = (keyColumnCount > 0);
    controlAttributesConfig->EnableEndOfStream = enableEndOfStream;
    return CreateWriterForSkiff(
        {std::move(skiffSchema)},
        std::move(nameTable),
        tableSchemaList,
        NConcurrency::CreateAsyncAdapter(outputStream),
        false,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

void TestAllWireTypes(bool useSchema)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double_1"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double_2"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("null"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("opt_int64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("opt_uint64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double_1"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double_2"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Boolean),
        })->SetName("opt_boolean"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("opt_string32"),
    });
    std::vector<TTableSchemaPtr> tableSchemas;
    if (useSchema) {
        tableSchemas.push_back(New<TTableSchema>(std::vector{
            TColumnSchema("int64", EValueType::Int64),
            TColumnSchema("uint64", EValueType::Uint64),
            TColumnSchema("double_1", EValueType::Double),
            TColumnSchema("double_2", ESimpleLogicalValueType::Float),
            TColumnSchema("boolean", EValueType::Boolean),
            TColumnSchema("string32", EValueType::String),
            TColumnSchema("null", EValueType::Null),
            TColumnSchema("opt_int64", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
            TColumnSchema("opt_uint64", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))),
            TColumnSchema("opt_double_1", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Double))),
            TColumnSchema("opt_double_2", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Float))),
            TColumnSchema("opt_boolean", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Boolean))),
            TColumnSchema("opt_string32", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::String))),
        }));
    } else {
        tableSchemas.push_back(New<TTableSchema>());
    }
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, tableSchemas);

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(-1, nameTable->GetIdOrRegisterName("int64")),
                MakeUnversionedUint64Value(2, nameTable->GetIdOrRegisterName("uint64")),
                MakeUnversionedDoubleValue(3.0, nameTable->GetIdOrRegisterName("double_1")),
                MakeUnversionedDoubleValue(3.0, nameTable->GetIdOrRegisterName("double_2")),
                MakeUnversionedBooleanValue(true, nameTable->GetIdOrRegisterName("boolean")),
                MakeUnversionedStringValue("four", nameTable->GetIdOrRegisterName("string32")),
                MakeUnversionedNullValue(nameTable->GetIdOrRegisterName("null")),

                MakeUnversionedInt64Value(-5, nameTable->GetIdOrRegisterName("opt_int64")),
                MakeUnversionedUint64Value(6, nameTable->GetIdOrRegisterName("opt_uint64")),
                MakeUnversionedDoubleValue(7.0, nameTable->GetIdOrRegisterName("opt_double_1")),
                MakeUnversionedDoubleValue(7.0, nameTable->GetIdOrRegisterName("opt_double_2")),
                MakeUnversionedBooleanValue(false, nameTable->GetIdOrRegisterName("opt_boolean")),
                MakeUnversionedStringValue("eight", nameTable->GetIdOrRegisterName("opt_string32")),
            }).Get(),
        });
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(-9, nameTable->GetIdOrRegisterName("int64")),
                MakeUnversionedUint64Value(10, nameTable->GetIdOrRegisterName("uint64")),
                MakeUnversionedDoubleValue(11.0, nameTable->GetIdOrRegisterName("double_1")),
                MakeUnversionedDoubleValue(11.0, nameTable->GetIdOrRegisterName("double_2")),
                MakeUnversionedBooleanValue(false, nameTable->GetIdOrRegisterName("boolean")),
                MakeUnversionedStringValue("twelve", nameTable->GetIdOrRegisterName("string32")),
                MakeUnversionedNullValue(nameTable->GetIdOrRegisterName("null")),

                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_int64")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_uint64")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_double_1")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_double_2")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_boolean")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_string32")),
            }).Get()
        });

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 2);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 3.0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 3.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "four");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -5);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 6);

    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 7.0);

    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 7.0);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "eight");

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -9);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 10);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 11.0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 11.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "twelve");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    // double_1
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    // double_2
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestAllWireTypesNoSchema)
{
    TestAllWireTypes(false);
}

TEST(TSkiffWriter, TestAllWireTypesWithSchema)
{
    TestAllWireTypes(true);
}

TEST(TSkiffWriter, TestYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson32"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Yson32),
        })->SetName("opt_yson32"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        // Row 0 (Null)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 1 (Int64)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedInt64Value(-5, nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedInt64Value(-6, nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 2 (Uint64)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedUint64Value(42, nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedUint64Value(43, nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 3 ((Double)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedDoubleValue(2.7182818, nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedDoubleValue(3.1415926, nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 4 ((Boolean)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedBooleanValue(true, nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedBooleanValue(false, nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 5 ((String)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedStringValue("Yin", nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedStringValue("Yang", nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 6 ((Any)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),

                MakeUnversionedAnyValue("{foo=bar;}", nameTable->GetIdOrRegisterName("yson32")),
                MakeUnversionedAnyValue("{bar=baz;}", nameTable->GetIdOrRegisterName("opt_yson32")),
            }).Get(),
        });

        // Row 7 ((missing optional values)
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            }).Get(),
        });

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    auto parseYson = [] (TCheckedSkiffParser* parser) {
        auto yson = TString{parser->ParseYson32()};
        return ConvertToNode(TYsonString(yson));
    };

    // Row 0 (Null)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->GetType(), ENodeType::Entity);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // Row 1 (Int64)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsInt64()->GetValue(), -5);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsInt64()->GetValue(), -6);

    // Row 2 (Uint64)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsUint64()->GetValue(), 42);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsUint64()->GetValue(), 43);

    // Row 3 (Double)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsDouble()->GetValue(), 2.7182818);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsDouble()->GetValue(), 3.1415926);

    // Row 4 (Boolean)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsBoolean()->GetValue(), true);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsBoolean()->GetValue(), false);

    // Row 5 (String)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsString()->GetValue(), "Yin");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsString()->GetValue(), "Yang");

    // Row 6 (Any)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChildOrThrow("foo")->AsString()->GetValue(), "bar");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChildOrThrow("bar")->AsString()->GetValue(), "baz");

    // Row 7 (Null)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->GetType(), ENodeType::Entity);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

class TSkiffFormatSmallIntP
: public ::testing::TestWithParam<std::tuple<
    std::shared_ptr<TSkiffSchema>,
    TLogicalTypePtr,
    TTableField::TValue,
    TString
>>
{
public:
    static std::vector<ParamType> GetCases()
    {
        using namespace NLogicalTypeShortcuts;

        std::vector<ParamType> result;

        auto addSimpleCase = [&result] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            auto simpleSkiffSchema = CreateSimpleTypeSchema(wireType);
            auto simpleSkiffData = TString(2, 0) + skiffValue;
            result.emplace_back(simpleSkiffSchema, logicalType, value, simpleSkiffData);
        };

        auto addListCase = [&result] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            auto listSkiffSchema = CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(wireType)});
            auto listSkiffData = TString(3, 0) + skiffValue + TString(1, '\xff');
            auto listValue = TTableField::TValue{
                TTableField::TComposite{
                    BuildYsonStringFluently()
                        .BeginList()
                        .Item().Value(value)
                        .EndList().ToString()
                }
            };
            result.emplace_back(listSkiffSchema, List(logicalType), listValue, listSkiffData);
        };

        auto addSimpleAndListCases = [&] (
            EWireType wireType,
            const TLogicalTypePtr& logicalType,
            auto value,
            TStringBuf skiffValue)
        {
            addSimpleCase(wireType, logicalType, value, skiffValue);
            addListCase(wireType, logicalType, value, skiffValue);
        };

        auto addMultiCase = [&] (EWireType wireType, auto value, TStringBuf skiffValue) {
            auto add = [&] (const TLogicalTypePtr& logicalType) {
                addSimpleAndListCases(wireType, logicalType, value, skiffValue);
            };
            addSimpleCase(wireType, Yson(), value, skiffValue);

            using T = std::decay_t<decltype(value)>;
            static_assert(std::is_integral_v<T>);
            if constexpr (std::is_signed_v<T>) {
                if (std::numeric_limits<i8>::min() <= value && value <= std::numeric_limits<i8>::max()) {
                    add(Int8());
                }
                if (std::numeric_limits<i16>::min() <= value && value <= std::numeric_limits<i16>::max()) {
                    add(Int16());
                }
                if (std::numeric_limits<i32>::min() <= value && value <= std::numeric_limits<i32>::max()) {
                    add(Int32());
                }
                add(Int64());
            } else {
                if (value <= std::numeric_limits<ui8>::max()) {
                    add(Uint8());
                }
                if (value <= std::numeric_limits<ui16>::max()) {
                    add(Uint16());
                }
                if (value <= std::numeric_limits<ui32>::max()) {
                    add(Uint32());
                }
                add(Uint64());
            }
        };
        addMultiCase(EWireType::Int8, 0, AsStringBuf("\x00"));
        addMultiCase(EWireType::Int8, 42, AsStringBuf("*"));
        addMultiCase(EWireType::Int8, -42, AsStringBuf("\xd6"));
        addMultiCase(EWireType::Int8, 127, AsStringBuf("\x7f"));
        addMultiCase(EWireType::Int8, -128, AsStringBuf("\x80"));

        addMultiCase(EWireType::Int16, 0, AsStringBuf("\x00\x00"));
        addMultiCase(EWireType::Int16, 42, AsStringBuf("\x2a\x00"));
        addMultiCase(EWireType::Int16, -42, AsStringBuf("\xd6\xff"));
        addMultiCase(EWireType::Int16, 0x7fff, AsStringBuf("\xff\x7f"));
        addMultiCase(EWireType::Int16, -0x8000, AsStringBuf("\x00\x80"));

        addMultiCase(EWireType::Int32, 0, AsStringBuf("\x00\x00\x00\x00"));
        addMultiCase(EWireType::Int32, 42, AsStringBuf("\x2a\x00\x00\x00"));
        addMultiCase(EWireType::Int32, -42, AsStringBuf("\xd6\xff\xff\xff"));
        addMultiCase(EWireType::Int32, 0x7fffffff, AsStringBuf("\xff\xff\xff\x7f"));
        addMultiCase(EWireType::Int32, -0x80000000l, AsStringBuf("\x00\x00\x00\x80"));

        addMultiCase(EWireType::Uint8, 0ull, AsStringBuf("\x00"));
        addMultiCase(EWireType::Uint8, 42ull, AsStringBuf("*"));
        addMultiCase(EWireType::Uint8, 255ull, AsStringBuf("\xff"));

        addMultiCase(EWireType::Uint16, 0ull, AsStringBuf("\x00\x00"));
        addMultiCase(EWireType::Uint16, 42ull, AsStringBuf("\x2a\x00"));
        addMultiCase(EWireType::Uint16, 0xFFFFull, AsStringBuf("\xff\xff"));

        addMultiCase(EWireType::Uint32, 0ull, AsStringBuf("\x00\x00\x00\x00"));
        addMultiCase(EWireType::Uint32, 42ull, AsStringBuf("\x2a\x00\x00\x00"));
        addMultiCase(EWireType::Uint32, 0xFFFFFFFFull, AsStringBuf("\xff\xff\xff\xff"));

        addSimpleAndListCases(EWireType::Uint16, Date(), 0ull, AsStringBuf("\x00\x00"));
        addSimpleAndListCases(EWireType::Uint16, Date(), 42ull, AsStringBuf("\x2a\x00"));
        addSimpleAndListCases(EWireType::Uint16, Date(), DateUpperBound - 1, AsStringBuf("\x08\xc2"));

        addSimpleAndListCases(EWireType::Uint32, Datetime(), 0ull, AsStringBuf("\x00\x00\x00\x00"));
        addSimpleAndListCases(EWireType::Uint32, Datetime(), 42ull, AsStringBuf("\x2a\x00\x00\x00"));
        addSimpleAndListCases(EWireType::Uint32, Datetime(), DatetimeUpperBound - 1, AsStringBuf("\x7f\xdd\xce\xff"));

        return result;
    }

    static const std::vector<ParamType> Cases;
};

const std::vector<TSkiffFormatSmallIntP::ParamType> TSkiffFormatSmallIntP::Cases = TSkiffFormatSmallIntP::GetCases();

INSTANTIATE_TEST_SUITE_P(
    Cases,
    TSkiffFormatSmallIntP,
    ::testing::ValuesIn(TSkiffFormatSmallIntP::Cases));

TEST_P(TSkiffFormatSmallIntP, Test)
{
    const auto& [skiffValueSchema, logicalType, value, expectedSkiffData] = GetParam();

    const auto nameTable = New<TNameTable>();

    TStringStream actualSkiffData;
    auto skiffTableSchema = CreateTupleSchema({
        skiffValueSchema->SetName("column")
    });
    auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema("column", logicalType),
    });
    auto writer = CreateSkiffWriter(skiffTableSchema, nameTable, &actualSkiffData, {tableSchema});
    writer->Write({
        MakeRow(nameTable, {{"column", value}})
    });
    writer->Close()
        .Get()
        .ThrowOnError();
    EXPECT_EQ(actualSkiffData.Str(), expectedSkiffData);

    TCollectingValueConsumer rowCollector(nameTable);
    auto parser = CreateParserForSkiff(skiffTableSchema, tableSchema, &rowCollector);
    parser->Read(expectedSkiffData);
    parser->Finish();
    auto actualValue = rowCollector.GetRowValue(0, "column");

    EXPECT_EQ(actualValue, TTableField("common", value).ToUnversionedValue(nameTable));
}

TEST(TSkiffWriter, TestBadSmallIntegers)
{
    using namespace NLogicalTypeShortcuts;
    auto writeSkiffValue = [] (
        std::shared_ptr<TSkiffSchema>&& typeSchema,
        TLogicalTypePtr logicalType,
        TTableField::TValue value)
    {
        TStringStream result;
        auto skiffSchema = CreateTupleSchema({
            typeSchema->SetName("column")
        });
        auto tableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("column", std::move(logicalType)),
        });
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
        writer->Write({
            MakeRow(nameTable, {{"column", std::move(value)}})
        });
        writer->Close()
            .Get()
            .ThrowOnError();
        return result.Str();
    };

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int8), Int64(), 128),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int8), Int64(), -129),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int16), Int64(), 0x8000),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int16), Int64(), -0x8001),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int32), Int64(), 0x80000000ll),
        "is out of range for possible values");
    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Int32), Int64(), -0x80000001ll),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint8), Uint64(), 256ull),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint16), Uint64(), 0x1FFFFull),
        "is out of range for possible values");

    EXPECT_THROW_WITH_SUBSTRING(
        writeSkiffValue(CreateSimpleTypeSchema(EWireType::Uint32), Uint64(), 0x100000000ull),
        "is out of range for possible values");
}

class TSkiffFormatUuidTestP : public ::testing::TestWithParam<std::tuple<
    TNameTablePtr,
    TTableSchemaPtr,
    std::shared_ptr<TSkiffSchema>,
    std::vector<TUnversionedOwningRow>,
    TString
>>
{
public:
    static std::vector<ParamType> GetCases()
    {
        using namespace NLogicalTypeShortcuts;

        auto nameTable = New<TNameTable>();
        const auto uuidValue = TStringBuf("\xee\x1f\x37\x70" "\xb9\x93\x64\xb5" "\xe4\xdf\xe9\x03" "\x67\x5c\x30\x62");
        const auto skiffUuidValue = TStringBuf("\x62\x30\x5c\x67" "\x03\xe9\xdf\xe4" "\xb5\x64\x93\xb9" "\x70\x37\x1f\xee");

        auto requiredTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("uuid", Uuid())});
        auto optionalTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("uuid", Optional(Uuid()))});

        auto optionalSkiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Uint128),
            })->SetName("uuid"),
        });

        auto requiredSkiffSchema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Uint128)->SetName("uuid"),
        });

        std::vector<ParamType> result;

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            requiredSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", uuidValue}}),
            },
            TString(2, '\0') + skiffUuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            requiredSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", uuidValue}}),
            },
            TString(2, '\0') + skiffUuidValue);

        result.emplace_back(
            nameTable,
            requiredTableSchema,
            optionalSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", uuidValue}}),
            },
            TString(2, '\0') + "\1" + skiffUuidValue);

        result.emplace_back(
            nameTable,
            optionalTableSchema,
            optionalSkiffSchema,
            std::vector<TUnversionedOwningRow>{
                MakeRow(nameTable, {{"uuid", uuidValue}}),
            },
            TString(2, '\0') + "\1" + skiffUuidValue);

        return result;
    }

    static const std::vector<ParamType> Cases;
};

const std::vector<TSkiffFormatUuidTestP::ParamType> TSkiffFormatUuidTestP::Cases = TSkiffFormatUuidTestP::GetCases();

INSTANTIATE_TEST_SUITE_P(
    Cases,
    TSkiffFormatUuidTestP,
    ::testing::ValuesIn(TSkiffFormatUuidTestP::Cases));

TEST_P(TSkiffFormatUuidTestP, Test)
{
    const auto& [nameTable, tableSchema, skiffSchema, rows, skiffString] = GetParam();

    TStringStream result;
    std::vector<TUnversionedRow> nonOwningRows;
    for (const auto& row : rows) {
        nonOwningRows.emplace_back(row);
    }
    auto skiffWriter = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
    skiffWriter->Write(MakeRange(nonOwningRows));
    skiffWriter->Close().Get().ThrowOnError();
    ASSERT_EQ(result.Str(), skiffString);

    TCollectingValueConsumer rowCollector(nameTable);
    auto requiredParser = CreateParserForSkiff(skiffSchema, tableSchema, &rowCollector);
    requiredParser->Read(result.Str());
    requiredParser->Finish();
    ASSERT_EQ(rowCollector.GetRowList(), rows);
}

TEST(TSkiffFormatUuidTest, TestError)
{
    using namespace NLogicalTypeShortcuts;

    auto nameTable = New<TNameTable>();
    auto tableSchema = New<TTableSchema>(
        std::vector<TColumnSchema>{TColumnSchema("uuid", Optional(Uuid()))});

    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Uint128)->SetName("uuid"),
    });

    TStringStream result;
    auto skiffWriter = CreateSkiffWriter(skiffSchema, nameTable, &result, {tableSchema});
    skiffWriter->Write({
        MakeRow(nameTable, {{"uuid", nullptr}}),
    });
    EXPECT_THROW_WITH_SUBSTRING(skiffWriter->Close().Get().ThrowOnError(),
        "Unexpected type");

}

class TSkiffWriterSingular
    : public ::testing::Test
    , public ::testing::WithParamInterface<ESimpleLogicalValueType>
{};

INSTANTIATE_TEST_SUITE_P(
    Singular,
    TSkiffWriterSingular,
    ::testing::Values(ESimpleLogicalValueType::Null, ESimpleLogicalValueType::Void));

TEST_P(TSkiffWriterSingular, TestOptionalSingular)
{
    const auto singularType = GetParam();

    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("opt_null"),
    });

    auto nameTable = New<TNameTable>();
    const std::vector<TTableSchemaPtr> tableSchemas = {
        New<TTableSchema>(std::vector{
            TColumnSchema("opt_null", OptionalLogicalType(SimpleLogicalType(singularType))),
        }),
    };

    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, tableSchemas);
        // Row 0
        writer->Write(
            {
                MakeRow(
                    {
                        MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                        MakeUnversionedNullValue(nameTable->GetIdOrRegisterName("opt_null")),
                    }).Get(),
            });
        // Row 1
        writer->Write(
            {
                MakeRow(
                    {
                        MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                        MakeUnversionedCompositeValue("[#]", nameTable->GetIdOrRegisterName("opt_null")),
                    }).Get(),
            });
        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);

    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestRearrange)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("eng"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("rus"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(1, nameTable->GetIdOrRegisterName("number")),
                MakeUnversionedStringValue("one", nameTable->GetIdOrRegisterName("eng")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("rus")),
            }).Get()
        });

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("eng")),
                MakeUnversionedInt64Value(2, nameTable->GetIdOrRegisterName("number")),
                MakeUnversionedStringValue("dva", nameTable->GetIdOrRegisterName("rus")),
            }).Get()
        });

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedStringValue("tri", nameTable->GetIdOrRegisterName("rus")),
                MakeUnversionedStringValue("three", nameTable->GetIdOrRegisterName("eng")),
                MakeUnversionedInt64Value(3, nameTable->GetIdOrRegisterName("number")),
            }).Get()
        });

        writer->Close()
            .Get()
            .ThrowOnError();
    }

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "dva");

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 3);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "three");
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "tri");

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestMissingRequiredField)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("eng"),
    });
    auto nameTable = New<TNameTable>();
    TString result;
    try {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(1, nameTable->GetIdOrRegisterName("number")),
            }).Get()
        });
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Unexpected type of \"eng\" column"));
    }
}

TEST(TSkiffWriter, TestSparse)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        })->SetName("$sparse_columns"),
    });

    auto nameTable = New<TNameTable>();
    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedInt64Value(-1, nameTable->GetIdOrRegisterName("int64")),
            MakeUnversionedStringValue("minus one", nameTable->GetIdOrRegisterName("string32")),
        }).Get(),
    });

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedStringValue("minus five", nameTable->GetIdOrRegisterName("string32")),
            MakeUnversionedInt64Value(-5, nameTable->GetIdOrRegisterName("int64")),
        }).Get(),
    });

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedUint64Value(42, nameTable->GetIdOrRegisterName("uint64")),
        }).Get(),
    });

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedInt64Value(-8, nameTable->GetIdOrRegisterName("int64")),
            MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("uint64")),
            MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("string32")),
        }).Get(),
    });

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(result);
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -1);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "minus one");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 2);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "minus five");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -5);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 42);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 3
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -8);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // row 4
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestMissingFields)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    });

    try {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedStringValue("four", nameTable->GetIdOrRegisterName("unknown_column")),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Column \"unknown_column\" is not described by Skiff schema"));
    }

    try {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto unknownColumnId = nameTable->RegisterName("unknown_column");
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()});

        ASSERT_TRUE(unknownColumnId < nameTable->GetId("value"));

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedStringValue("four", nameTable->GetIdOrRegisterName("unknown_column")),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Column \"unknown_column\" is not described by Skiff schema"));
    }
}

TEST(TSkiffWriter, TestOtherColumns)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64)
        })->SetName("int64_column"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    nameTable->RegisterName("string_column");
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()});

    // Row 0.
    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedStringValue("foo", nameTable->GetIdOrRegisterName("string_column")),
        }).Get(),
    });
    // Row 1.
    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedInt64Value(42, nameTable->GetIdOrRegisterName("int64_column")),
        }).Get(),
    });
    // Row 2.
    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            MakeUnversionedStringValue("bar", nameTable->GetIdOrRegisterName("other_string_column")),
        }).Get(),
    });
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    auto parseYson = [] (TCheckedSkiffParser* parser) {
        auto yson = TString{parser->ParseYson32()};
        return ConvertToYsonTextStringStable(ConvertToNode(TYsonString(yson)));
    };

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{\"string_column\"=\"foo\";}");

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), 42);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{}");

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser), "{\"other_string_column\"=\"bar\";}");

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestKeySwitch)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1);

    writer->Write({
        // Row 0.
        MakeRow({
            MakeUnversionedStringValue("one", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    // Row 1.
    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("one", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    // Row 2.
    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("two", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    TString buf;

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    // row 1
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);

    // row 2
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "two");
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestEndOfStream)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1, true);

    // Row 0.
    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("zero", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    // Row 1.
    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("one", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    TString buf;

    // Row 0.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "zero");

    // Row 1.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");

    // End of stream.
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0xffff);

    // The End.
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestRowRangeIndex)
{
    const auto rowAndRangeIndex = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$range_index"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$row_index"),
    });

    struct TRow {
        int TableIndex;
        std::optional<int> RangeIndex;
        std::optional<int> RowIndex;
    };
    auto generateUnversionedRow = [] (const TRow& row, const TNameTablePtr& nameTable) {
        std::vector<TUnversionedValue> values = {
            MakeUnversionedInt64Value(row.TableIndex, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        };
        if (row.RangeIndex) {
            values.emplace_back(
                MakeUnversionedInt64Value(*row.RangeIndex, nameTable->GetIdOrRegisterName(RangeIndexColumnName))
            );
        }
        if (row.RowIndex) {
            values.emplace_back(
                MakeUnversionedInt64Value(*row.RowIndex, nameTable->GetIdOrRegisterName(RowIndexColumnName))
            );
        }
        return MakeRow(values);
    };

    auto skiffWrite = [generateUnversionedRow] (const std::vector<TRow>& rows, const std::shared_ptr<TSkiffSchema>& skiffSchema) {
        std::vector<TTableSchemaPtr> tableSchemas;
        {
            THashSet<int> tableIndices;
            for (const auto& row : rows) {
                tableIndices.insert(row.TableIndex);
            }
            tableSchemas.assign(tableIndices.size(), New<TTableSchema>());
        }


        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(
            skiffSchema,
            nameTable,
            &resultStream,
            tableSchemas);

        for (const auto& row : rows) {
            writer->Write({generateUnversionedRow(row, nameTable)});
        }
        writer->Close()
            .Get()
            .ThrowOnError();

        return HexEncode(resultStream.Str());
    };

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 2},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "00"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 3},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "01""03000000""00000000"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 2},
            {0, 1, 3},
        }, rowAndRangeIndex).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "01""01000000""00000000" "01""02000000""00000000"
        "0000" "00" "00"
    );

    EXPECT_THROW_WITH_SUBSTRING(skiffWrite({{0, 0, {}}}, rowAndRangeIndex), "index requested but reader did not return it");
    EXPECT_THROW_WITH_SUBSTRING(skiffWrite({{0, {}, 0}}, rowAndRangeIndex), "index requested but reader did not return it");

    const auto rowAndRangeIndexAllowMissing = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("$range_index"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("$row_index"),
    });

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 2},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "00"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 0, 3},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "00" "01""03000000""00000000"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, 0},
            {0, 0, 1},
            {0, 1, 2},
            {0, 1, 3},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "01""00000000""00000000"
        "0000" "00" "00"
        "0000" "01""01000000""00000000" "01""02000000""00000000"
        "0000" "00" "00"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, {}, {}},
            {0, {}, {}},
            {0, {}, {}},
            {0, {}, {}},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "02" "02"
        "0000" "02" "02"
        "0000" "02" "02"
        "0000" "02" "02"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, {}, 0},
            {0, {}, 1},
            {0, {}, 3},
            {0, {}, 4},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "02" "01""00000000""00000000"
        "0000" "02" "00"
        "0000" "02" "01""03000000""00000000"
        "0000" "02" "00"
    );

    EXPECT_STREQ(
        skiffWrite({
            {0, 0, {}},
            {0, 0, {}},
            {0, 1, {}},
            {0, 1, {}},
        }, rowAndRangeIndexAllowMissing).data(),

        "0000" "01""00000000""00000000" "02"
        "0000" "00" "02"
        "0000" "01""01000000""00000000" "02"
        "0000" "00" "02"
    );
}

TEST(TSkiffWriter, TestRowIndexOnlyOrRangeIndexOnly)
{
    TString columnNameList[] = {
        RowIndexColumnName,
        RangeIndexColumnName,
    };

    for (const auto& columnName : columnNameList) {
        auto skiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Int64),
            })->SetName(columnName),
        });

        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, {New<TTableSchema>()}, 1);

        // Row 0.
        writer->Write({
            MakeRow(
                {
                    MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(columnName)),
                }).Get(),
            });
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
            CreateRepeatedVariant8Schema({
                CreateTupleSchema({
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("x"),
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("y"),
                })
            })->SetName("points")
        })->SetName("value"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", StructLogicalType({
                {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                {
                    "points",
                    ListLogicalType(
                        StructLogicalType({
                            {"x", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                            {"y", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
                        })
                    )
                }
            })),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        writer->Write({
            MakeRow({
                MakeUnversionedCompositeValue("[foo;[[0; 1];[2;3]]]", nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 3);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), EndOfSequenceTag<ui8>());

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestEmptyComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
            })
        })->SetName("value"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                }))
            ),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        writer->Write({
            MakeRow({
                MakeUnversionedNullValue(nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestSparseComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto tableSchema = New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value",   SimpleLogicalType(ESimpleLogicalValueType::String)},
                }))
            ),
        });
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

        // Row 0.
        writer->Write({
            MakeRow({
                MakeUnversionedCompositeValue("[foo;bar;]", nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "bar");
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestSparseComplexTypeWithExtraOptional)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateTupleSchema({
                        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
                })
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    TStringStream resultStream;
    auto nameTable = New<TNameTable>();
    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("value", OptionalLogicalType(
            StructLogicalType({
                {"name", SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"value", SimpleLogicalType(ESimpleLogicalValueType::String)},
            }))
        ),
    });

    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{tableSchema});

    // Row 0.
    writer->Write({
        MakeRow({
            MakeUnversionedCompositeValue("[foo;bar;]", nameTable->GetIdOrRegisterName("value")),
            MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
        }).Get(),
    });
    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput resultInput(resultStream.Str());
    TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

    // row 0
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "foo");
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "bar");
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), EndOfSequenceTag<ui16>());

    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
}

TEST(TSkiffWriter, TestBadWireTypeForSimpleColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Yson32),
            })
        })->SetName("opt_yson32"),
    });
    auto nameTable = New<TNameTable>();
    TStringStream resultStream;
    EXPECT_THROW_WITH_SUBSTRING(
        CreateSkiffWriter(skiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()}),
        "cannot be represented with skiff schema"
    );
}

TEST(TSkiffWriter, TestMissingComplexColumn)
{
    auto optionalSkiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Int64)}),
        })->SetName("opt_list"),
    });
    auto requiredSkiffSchema = CreateTupleSchema({
        CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Int64)})->SetName("opt_list"),
    });

    { // Non optional skiff schema
        auto nameTable = New<TNameTable>();
        EXPECT_THROW_WITH_SUBSTRING(
            CreateSkiffWriter(requiredSkiffSchema, nameTable, &Cnull, std::vector{New<TTableSchema>()}),
            "cannot be represented with skiff schema"
        );
    }

    {
        auto nameTable = New<TNameTable>();
        TStringStream resultStream;
        auto writer = CreateSkiffWriter(optionalSkiffSchema, nameTable, &resultStream, std::vector{New<TTableSchema>()});
        writer->Write({
            MakeRow({ }).Get(),
            MakeRow({ MakeUnversionedNullValue(nameTable->GetIdOrRegisterName("opt_list")), }).Get(),
            MakeRow({ }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();

        EXPECT_EQ(HexEncode(resultStream.Str()), "0000" "00" "0000" "00" "0000" "00");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, Simple)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("null"),

        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("opt_int64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Uint64),
        })->SetName("opt_uint64"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Double),
        })->SetName("opt_double"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Boolean),
        })->SetName("opt_boolean"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::String32),
        })->SetName("opt_string32"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-1);
    checkedSkiffWriter.WriteUint64(2);
    checkedSkiffWriter.WriteDouble(3.0);
    checkedSkiffWriter.WriteBoolean(true);
    checkedSkiffWriter.WriteString32("foo");

    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 1);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "int64")), -1);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "uint64")), 2);
    ASSERT_EQ(GetDouble(collectedRows.GetRowValue(0, "double")), 3.0);
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(0, "boolean")), true);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "string32")), "foo");
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "null")), true);

    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_int64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_uint64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_double")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_boolean")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_string32")), true);
}

TEST(TSkiffParser, TestOptionalNull)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Nothing),
        })->SetName("opt_null"),
    });
    auto nameTable = New<TNameTable>();

    {
        TCollectingValueConsumer collectedRows;
        EXPECT_THROW_WITH_SUBSTRING(
            CreateParserForSkiff(skiffSchema, &collectedRows),
            "cannot be represented with skiff schema");
    }

    auto tableSchema = New<TTableSchema>(std::vector{
        TColumnSchema("opt_null", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))),
    });

    TCollectingValueConsumer collectedRows(tableSchema);
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(0);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(1);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);

    ASSERT_EQ(collectedRows.GetRowValue(0, "opt_null").Type, EValueType::Null);
}

TEST(TSkiffParser, TestSparse)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // row 1
    checkedSkiffWriter.WriteVariant16Tag(0);
    // sparse fields begin
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-42);
    checkedSkiffWriter.WriteVariant16Tag(1);
    checkedSkiffWriter.WriteUint64(54);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // row 2
    checkedSkiffWriter.WriteVariant16Tag(0);
    // sparse fields begin
    checkedSkiffWriter.WriteVariant16Tag(2);
    checkedSkiffWriter.WriteString32("foo");
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "int64")), -42);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "uint64")), 54);
    ASSERT_FALSE(collectedRows.FindRowValue(0, "string32"));

    ASSERT_FALSE(collectedRows.FindRowValue(1, "int64"));
    ASSERT_FALSE(collectedRows.FindRowValue(1, "uint64"));
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "string32")), "foo");
}

TEST(TSkiffParser, TestYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("-42");

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("42u");

    // Row 2.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("\"foobar\"");

    // Row 3.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("%true");

    // Row 4.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("{foo=bar}");

    // Row 5.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteYson32("#");

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 6);
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "yson")), -42);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(1, "yson")), 42);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(2, "yson")), "foobar");
    ASSERT_EQ(GetBoolean(collectedRows.GetRowValue(3, "yson")), true);
    ASSERT_EQ(GetAny(collectedRows.GetRowValue(4, "yson"))->AsMap()->GetChildOrThrow("foo")->AsString()->GetValue(), "bar");
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(5, "yson")), true);
}

TEST(TSkiffParser, TestBadYsonWireType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
    });

    auto parseYsonUsingSkiff = [&] (TStringBuf ysonValue) {
        TCollectingValueConsumer collectedRows;
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        TStringStream dataStream;
        ASSERT_NO_THROW({
            TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

            checkedSkiffWriter.WriteVariant16Tag(0);
            checkedSkiffWriter.WriteYson32(ysonValue);

            checkedSkiffWriter.Finish();
        });

        parser->Read(dataStream.Str());
        parser->Finish();
    };

    try {
        parseYsonUsingSkiff("[42");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Premature end of stream"));
    }

    try {
        parseYsonUsingSkiff("<foo=bar>42");
    } catch (const std::exception& e) {
        EXPECT_THAT(e.what(), testing::HasSubstr("Table values cannot have top-level attributes"));
    }
}

TEST(TSkiffParser, TestSpecialColumns)
{
    std::shared_ptr<TSkiffSchema> skiffSchemaList[] = {
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$key_switch"),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$row_switch"),
        }),
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson"),
            CreateSimpleTypeSchema(EWireType::Boolean)->SetName("$range_switch"),
        }),
    };

    for (const auto& skiffSchema : skiffSchemaList) {
        try {
            TCollectingValueConsumer collectedRows;
            auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        } catch (std::exception& e) {
            EXPECT_THAT(e.what(), testing::HasSubstr("Skiff parser does not support \"$key_switch\""));
        }
    }
}

TEST(TSkiffParser, TestOtherColumns)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("$other_columns"),
    });

    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteYson32("{foo=-42;}");

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_1");
    checkedSkiffWriter.WriteYson32("{bar=qux;baz={boolean=%false;};}");

    // Row 2.
    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);
    ASSERT_EQ(GetString(collectedRows.GetRowValue(0, "name")), "row_0");
    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "foo")), -42);

    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "name")), "row_1");
    ASSERT_EQ(GetString(collectedRows.GetRowValue(1, "bar")), "qux");
    ASSERT_EQ(ConvertToYsonTextStringStable(GetAny(collectedRows.GetRowValue(1, "baz"))), "{\"boolean\"=%false;}");
}

TEST(TSkiffParser, TestComplexColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
        })->SetName("column")
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("column", NTableClient::StructLogicalType({
                {"key", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::String)},
                {"value", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::Int64)}
            }))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(42);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 1);
    ASSERT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "column"))), "[\"row_0\";42;]");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, TestEmptyInput)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("column"),
    });

    TCollectingValueConsumer collectedRows;

    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Finish();
        ASSERT_EQ(collectedRows.Size(), 0);
    }
    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Read("");
        parser->Finish();
        ASSERT_EQ(collectedRows.Size(), 0);
    }
    {
        auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);
        parser->Read("");
        parser->Read("");
        parser->Finish();
        ASSERT_EQ(collectedRows.Size(), 0);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, ColumnIds)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("field_a"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("field_b")
    });

    TCollectingValueConsumer collectedRows;
    collectedRows.GetNameTable()->GetIdOrRegisterName("field_b");
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteInt64(-1);
    checkedSkiffWriter.WriteUint64(2);

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 1);

    ASSERT_EQ(GetInt64(collectedRows.GetRowValue(0, "field_a")), -1);
    ASSERT_EQ(GetUint64(collectedRows.GetRowValue(0, "field_b")), 2);
}

TEST(TSkiffParser, TestSparseComplexType)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
                CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
            })->SetName("value"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("value", OptionalLogicalType(
                StructLogicalType({
                    {"name", SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value", SimpleLogicalType(ESimpleLogicalValueType::Int64)}
                })
            ))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(10);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);
    EXPECT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "value"))), "[\"row_0\";10;]");
    EXPECT_FALSE(collectedRows.FindRowValue(1, "value"));
}

TEST(TSkiffParser, TestSparseComplexTypeWithExtraOptional)
{
    auto skiffSchema = CreateTupleSchema({
        CreateRepeatedVariant16Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateTupleSchema({
                    CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("value"),
                })
            })->SetName("column"),
        })->SetName("$sparse_columns"),
    });

    TCollectingValueConsumer collectedRows(
        New<TTableSchema>(std::vector{
            TColumnSchema("column", OptionalLogicalType(
                StructLogicalType({
                    {"key", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::String)},
                    {"value", NTableClient::SimpleLogicalType(ESimpleLogicalValueType::Int64)}
                })
            ))
        }));
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    TStringStream dataStream;
    TCheckedSkiffWriter checkedSkiffWriter(CreateVariant16Schema({skiffSchema}), &dataStream);

    // Row 0.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant8Tag(1);
    checkedSkiffWriter.WriteString32("row_0");
    checkedSkiffWriter.WriteInt64(42);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    // Row 1.
    checkedSkiffWriter.WriteVariant16Tag(0);
    checkedSkiffWriter.WriteVariant16Tag(EndOfSequenceTag<ui16>());

    checkedSkiffWriter.Finish();

    parser->Read(dataStream.Str());
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);
    ASSERT_EQ(ConvertToYsonTextStringStable(GetComposite(collectedRows.GetRowValue(0, "column"))), "[\"row_0\";42;]");
    ASSERT_FALSE(collectedRows.FindRowValue(1, "column"));
}


TEST(TSkiffParser, TestBadWireTypeForSimpleColumn)
{
    auto skiffSchema = CreateTupleSchema({
        CreateVariant8Schema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Yson32),
            })
        })->SetName("opt_yson32"),
    });

    TCollectingValueConsumer collectedRows;
    EXPECT_THROW_WITH_SUBSTRING(
        CreateParserForSkiff(skiffSchema, &collectedRows),
        "cannot be represented with skiff schema"
    );
}

TEST(TSkiffParser, TestEmptyColumns)
{
    auto skiffSchema = CreateTupleSchema({});
    TCollectingValueConsumer collectedRows;
    auto parser = CreateParserForSkiff(skiffSchema, &collectedRows);

    parser->Read(AsStringBuf("\x00\x00\x00\x00"));
    parser->Finish();

    ASSERT_EQ(collectedRows.Size(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT