#include <yt/core/test_framework/framework.h>

#include "row_helpers.h"

#include <yt/ytlib/formats/config.h>
#include <yt/ytlib/formats/parser.h>
#include <yt/ytlib/formats/skiff_parser.h>
#include <yt/ytlib/formats/skiff_writer.h>
#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/table_client/public.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/skiff/schema_match.h>

#include <yt/core/skiff/skiff.h>
#include <yt/core/skiff/skiff_schema.h>

#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/tree_visitor.h>

namespace NYT {
namespace {

using namespace NFormats;
using namespace NSkiff;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TString ConvertToSkiffSchemaShortDebugString(INodePtr node)
{
    auto skiffFormatConfig = ConvertTo<TSkiffFormatConfigPtr>(node);
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

TString ConvertToYsonTextStringStable(INodePtr node)
{
    TStringStream out;
    TYsonWriter writer(&out, EYsonFormat::Text);
    VisitTree(node, &writer, Null, true);
    writer.Flush();
    return out.Str();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffSchemaParse, TestAllowedTypes)
{
    EXPECT_EQ(
        "{Uint64,}",

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
        "{String32,}",

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
        "{Variant8<String32;Int64;>,}",

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
        "{Variant8<Int64;String32;>,}",

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
        EXPECT_THAT(e.what(), testing::HasSubstr("recursive types are forbiden"));
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

    EXPECT_EQ(denseFieldDescriptionList[0].Name, "Foo");
    EXPECT_EQ(denseFieldDescriptionList[0].DeoptionalizedSchema->GetWireType(), EWireType::Uint64);
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
        EXPECT_EQ(tableDescriptionList[0].KeySwitchFieldIndex, TNullable<size_t>(1));
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
    NSkiff::TSkiffSchemaPtr skiffSchema,
    TNameTablePtr nameTable,
    IOutputStream* outputStream,
    int keyColumnCount = 0)
{
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableKeySwitch = (keyColumnCount > 0);
    return CreateSchemalessWriterForSkiff(
        {skiffSchema},
        nameTable,
        NConcurrency::CreateAsyncAdapter(outputStream),
        false,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffWriter, TestAllWireTypes)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),

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
    auto nameTable = New<TNameTable>();
    TString result;
    {
        TStringOutput resultStream(result);
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(-1, nameTable->GetIdOrRegisterName("int64")),
                MakeUnversionedUint64Value(2, nameTable->GetIdOrRegisterName("uint64")),
                MakeUnversionedDoubleValue(3.0, nameTable->GetIdOrRegisterName("double")),
                MakeUnversionedBooleanValue(true, nameTable->GetIdOrRegisterName("boolean")),
                MakeUnversionedStringValue("four", nameTable->GetIdOrRegisterName("string32")),

                MakeUnversionedInt64Value(-5, nameTable->GetIdOrRegisterName("opt_int64")),
                MakeUnversionedUint64Value(6, nameTable->GetIdOrRegisterName("opt_uint64")),
                MakeUnversionedDoubleValue(7.0, nameTable->GetIdOrRegisterName("opt_double")),
                MakeUnversionedBooleanValue(false, nameTable->GetIdOrRegisterName("opt_boolean")),
                MakeUnversionedStringValue("eight", nameTable->GetIdOrRegisterName("opt_string32")),
            }).Get(),
        });
        writer->Write({
            MakeRow({
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(-9, nameTable->GetIdOrRegisterName("int64")),
                MakeUnversionedUint64Value(10, nameTable->GetIdOrRegisterName("uint64")),
                MakeUnversionedDoubleValue(11.0, nameTable->GetIdOrRegisterName("double")),
                MakeUnversionedBooleanValue(false, nameTable->GetIdOrRegisterName("boolean")),
                MakeUnversionedStringValue("twelve", nameTable->GetIdOrRegisterName("string32")),

                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_int64")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_uint64")),
                MakeUnversionedSentinelValue(EValueType::Null, nameTable->GetIdOrRegisterName("opt_double")),
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
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 3.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), true);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "four");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseInt64(), -5);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(checkedSkiffParser.ParseUint64(), 6);

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
    ASSERT_EQ(checkedSkiffParser.ParseDouble(), 11.0);
    ASSERT_EQ(checkedSkiffParser.ParseBoolean(), false);
    ASSERT_EQ(checkedSkiffParser.ParseString32(), "twelve");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
    ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
    checkedSkiffParser.ValidateFinished();
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
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
        TString ysonBuffer;
        parser->ParseYson32(&ysonBuffer);
        return ConvertToNode(TYsonString(ysonBuffer));
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
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChild("foo")->AsString()->GetValue(), "bar");

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->AsMap()->GetChild("bar")->AsString()->GetValue(), "baz");

    // Row 7 (Null)
    ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
    ASSERT_EQ(parseYson(&checkedSkiffParser)->GetType(), ENodeType::Entity);

    ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

    // end
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
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream);

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
        TString ysonBuffer;
        parser->ParseYson32(&ysonBuffer);
        return ConvertToYsonTextStringStable(ConvertToNode(TYsonString(ysonBuffer)));
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
    auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, 1);

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

TEST(TSkiffWriter, TestRowRangeIndex)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$range_index"),
        CreateVariant8Schema({
            CreateSimpleTypeSchema(EWireType::Nothing),
            CreateSimpleTypeSchema(EWireType::Int64),
        })->SetName("$row_index"),
    });

    {
        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, 1);

        // Row 0.
        writer->Write({
            MakeRow({
                MakeUnversionedStringValue("zero", nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(RangeIndexColumnName)),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(RowIndexColumnName)),
            }).Get(),
        });
        // Row 1 next row.
        writer->Write({
            MakeRow({
                MakeUnversionedStringValue("one", nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(RangeIndexColumnName)),
                MakeUnversionedInt64Value(1, nameTable->GetIdOrRegisterName(RowIndexColumnName)),
            }).Get(),
        });
        // Row 2 next range.
        writer->Write({
            MakeRow({
                MakeUnversionedStringValue("two", nameTable->GetIdOrRegisterName("value")),
                MakeUnversionedInt64Value(0, nameTable->GetIdOrRegisterName(TableIndexColumnName)),
                MakeUnversionedInt64Value(1, nameTable->GetIdOrRegisterName(RangeIndexColumnName)),
                MakeUnversionedInt64Value(2, nameTable->GetIdOrRegisterName(RowIndexColumnName)),
            }).Get(),
        });
        writer->Close()
            .Get()
            .ThrowOnError();

        TStringInput resultInput(resultStream.Str());
        TCheckedSkiffParser checkedSkiffParser(CreateVariant16Schema({skiffSchema}), &resultInput);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "zero");
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 0);

        // row 1
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "one");
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 0);

        // row 0
        ASSERT_EQ(checkedSkiffParser.ParseVariant16Tag(), 0);
        ASSERT_EQ(checkedSkiffParser.ParseString32(), "two");
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseVariant8Tag(), 1);
        ASSERT_EQ(checkedSkiffParser.ParseInt64(), 2);

        ASSERT_EQ(checkedSkiffParser.HasMoreData(), false);
        checkedSkiffParser.ValidateFinished();
    }
}

TEST(TSkiffWriter, TestRowIndexOnlyOrRangeIndexOnly)
{
    TString columnNameList[] = {
        RowIndexColumnName,
        RangeIndexColumnName,
    };

    for (const auto columnName : columnNameList) {
        auto skiffSchema = CreateTupleSchema({
            CreateVariant8Schema({
                CreateSimpleTypeSchema(EWireType::Nothing),
                CreateSimpleTypeSchema(EWireType::Int64),
            })->SetName(columnName),
        });

        TStringStream resultStream;
        auto nameTable = New<TNameTable>();
        auto writer = CreateSkiffWriter(skiffSchema, nameTable, &resultStream, 1);

        // Row 0.
        writer->Write({
            MakeRow({
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

////////////////////////////////////////////////////////////////////////////////

TEST(TSkiffParser, Simple)
{
    auto skiffSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName("boolean"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),

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

    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_int64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_uint64")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_double")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_boolean")), true);
    ASSERT_EQ(IsNull(collectedRows.GetRowValue(0, "opt_string32")), true);
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
    ASSERT_EQ(collectedRows.FindRowValue(0, "string32").HasValue(), false);

    ASSERT_EQ(collectedRows.FindRowValue(1, "int64").HasValue(), false);
    ASSERT_EQ(collectedRows.FindRowValue(1, "uint64").HasValue(), false);
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
    ASSERT_EQ(GetAny(collectedRows.GetRowValue(4, "yson"))->AsMap()->GetChild("foo")->AsString()->GetValue(), "bar");
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
    TSkiffSchemaPtr skiffSchemaList[] = {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
