#include <yt/core/test_framework/framework.h>

#include <yt/client/formats/web_json_writer.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/json/json_parser.h>

#include <yt/core/ytree/fluent.h>

#include <limits>

namespace NYT::NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NTableClient;

INodePtr ParseJsonToNode(TStringBuf string)
{
    TBuildingYsonConsumerViaTreeBuilder<INodePtr> builder(EYsonType::Node);
    TMemoryInput stream(string);

    // For plain (raw) JSON parsing we need to switch off
    // "smart" attribute analysis and UTF-8 decoding.
    auto config = New<NJson::TJsonFormatConfig>();
    config->EncodeUtf8 = false;
    config->Plain = true;

    NJson::ParseJson(&stream, &builder, std::move(config));
    return builder.Finish();
}

template <typename TBase>
class TWriterForWebJsonBase
    : public TBase
{
protected:
    TNameTablePtr NameTable_;
    TWebJsonFormatConfigPtr Config_;

    TStringStream OutputStream_;

    ISchemalessFormatWriterPtr Writer_;

    int KeyAId_ = -1;
    int KeyBId_ = -1;
    int KeyCId_ = -1;
    int KeyDId_ = -1;

    int TableIndexColumnId_ = -1;
    int RowIndexColumnId_ = -1;
    int TabletIndexColumnId_ = -1;

    TWriterForWebJsonBase()
    {
        NameTable_ = New<TNameTable>();

        KeyAId_ = NameTable_->RegisterName("column_a");
        KeyBId_ = NameTable_->RegisterName("column_b");
        KeyCId_ = NameTable_->RegisterName("column_c");
        // We do not register KeyD intentionally.

        TableIndexColumnId_ = NameTable_->RegisterName(TableIndexColumnName);
        RowIndexColumnId_ = NameTable_->RegisterName(RowIndexColumnName);
        TabletIndexColumnId_ = NameTable_->RegisterName(TabletIndexColumnName);

        Config_ = New<TWebJsonFormatConfig>();
    }

    void CreateStandardWriter(const std::vector<TTableSchema>& schemas = {TTableSchema()})
    {
        Writer_ = CreateWriterForWebJson(
            Config_,
            NameTable_,
            schemas,
            CreateAsyncAdapter(static_cast<IOutputStream*>(&OutputStream_)));
    }
};

class TWriterForWebJson
    : public TWriterForWebJsonBase<::testing::Test>
{ };

TEST_F(TWriterForWebJson, Simple)
{
    Config_->MaxAllColumnNamesCount = 2;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
    row1.AddValue(MakeUnversionedBooleanValue(true, KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));
    row1.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));
    row2.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"uint64\","
                        "\"$value\":\"100500\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"true\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceColumnsByMaxCount)
{
    Config_->MaxSelectedColumnCount = 2;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));
    row1.AddValue(MakeUnversionedStringValue("row1_b", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "}"
                "},"
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceStrings)
{
    Config_->FieldWeightLimit = 6;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedStringValue("row1_b", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("rooooow1_c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("rooow2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_b\""
                    "},"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooooo\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_c\""
                    "},"
                    "\"column_b\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"string\","
                        "\"$value\":\"rooow2\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, ReplaceAnyWithNull)
{
    Config_->FieldWeightLimit = 8;

    CreateStandardWriter();

    TUnversionedRowBuilder row1;
    row1.AddValue(MakeUnversionedAnyValue("{key=a}", KeyBId_));
    row1.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));
    row1.AddValue(MakeUnversionedStringValue("row1_a", KeyAId_));

    TUnversionedRowBuilder row2;
    row2.AddValue(MakeUnversionedAnyValue("{key=aaaaaa}", KeyCId_));
    row2.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));

    TUnversionedRowBuilder row3;
    row3.AddValue(MakeUnversionedStringValue("row3_c", KeyCId_));

    std::vector<TUnversionedRow> rows = {row1.GetRow(), row2.GetRow(), row3.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"key\":{"
                            "\"$type\":\"string\","
                            "\"$value\":\"a\""
                        "}"
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_c\""
                    "},"
                    "\"column_a\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row1_a\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$incomplete\":true,"
                        "\"$type\":\"any\","
                        "\"$value\":\"\""
                    "},"
                    "\"column_b\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row2_b\""
                    "}"
                "},"
                "{"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"row3_c\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SkipSystemColumns)
{
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedInt64Value(0, TableIndexColumnId_));
    row.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));
    row.AddValue(MakeUnversionedInt64Value(2, TabletIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"$$table_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"0\""
                    "},"
                    "\"$$row_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"1\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"2\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$row_index\","
                "\"$table_index\","
                "\"$tablet_index\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SkipUnregisteredColumns)
{
    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedBooleanValue(true, KeyDId_));
    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));

    KeyDId_ = NameTable_->RegisterName("column_d");

    rows.clear();
    row.Reset();
    row.AddValue(MakeUnversionedBooleanValue(true, KeyDId_));
    rows.push_back(row.GetRow());

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                "},"
                "{"
                    "\"column_d\":{"
                        "\"$type\":\"boolean\","
                        "\"$value\":\"true\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"false\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"column_d\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

TEST_F(TWriterForWebJson, SliceColumnsByName)
{
    Config_->ColumnNames = {
        "column_b",
        "column_c",
        "$tablet_index"};
    Config_->MaxSelectedColumnCount = 2;
    Config_->SkipSystemColumns = false;

    CreateStandardWriter();

    TUnversionedRowBuilder row;
    row.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
    row.AddValue(MakeUnversionedDoubleValue(0.42, KeyBId_));
    row.AddValue(MakeUnversionedStringValue("abracadabra", KeyCId_));
    row.AddValue(MakeUnversionedInt64Value(10, TabletIndexColumnId_));

    std::vector<TUnversionedRow> rows = {row.GetRow()};

    EXPECT_EQ(true, Writer_->Write(rows));
    Writer_->Close();

    auto result = ParseJsonToNode(OutputStream_.Str());

    TString expectedOutput =
        "{"
            "\"rows\":["
                "{"
                    "\"column_b\":{"
                        "\"$type\":\"double\","
                        "\"$value\":\"0.42\""
                    "},"
                    "\"column_c\":{"
                        "\"$type\":\"string\","
                        "\"$value\":\"abracadabra\""
                    "},"
                    "\"$$tablet_index\":{"
                        "\"$type\":\"int64\","
                        "\"$value\":\"10\""
                    "}"
                "}"
            "],"
            "\"incomplete_columns\":\"true\","
            "\"incomplete_all_column_names\":\"false\","
            "\"all_column_names\":["
                "\"$tablet_index\","
                "\"column_a\","
                "\"column_b\","
                "\"column_c\""
            "]"
        "}";

    EXPECT_EQ(expectedOutput.length(), Writer_->GetWrittenSize());
    EXPECT_EQ(expectedOutput, OutputStream_.Str());
}

template <typename TValue>
void CheckYqlValue(
    const INodePtr& valueNode,
    const TValue& expectedValue)
{
    using TDecayedValue = std::decay_t<TValue>;
    if constexpr (std::is_convertible_v<TDecayedValue, TString>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::String);
        EXPECT_EQ(valueNode->GetValue<TString>(), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, double>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::String);
        EXPECT_FLOAT_EQ(FromString<double>(valueNode->GetValue<TString>()), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, bool>) {
        ASSERT_EQ(valueNode->GetType(), ENodeType::Boolean);
        EXPECT_EQ(valueNode->GetValue<bool>(), expectedValue);
    } else if constexpr (std::is_same_v<TDecayedValue, INodePtr>) {
        INodePtr actualValueNode;
        if (valueNode->GetType() == ENodeType::String) {
            // It is ["DataType", "Yson"], so we must interpret the string as YSON.
            actualValueNode = ConvertToNode(TYsonString(valueNode->GetValue<TString>()));
        } else {
            actualValueNode = valueNode;
        }
        EXPECT_TRUE(AreNodesEqual(actualValueNode, expectedValue))
            << "actualValueNode is " << ConvertToYsonString(actualValueNode, EYsonFormat::Pretty).GetData()
            << "\nexpectedValue is " << ConvertToYsonString(expectedValue, EYsonFormat::Pretty).GetData();
    } else {
        static_assert(TDependentFalse<TDecayedValue>::value, "Type not allowed");
    }
}

template <typename TType>
void CheckYqlType(
    const INodePtr& typeNode,
    const TType& expectedType,
    const std::vector<INodePtr>& yqlTypes)
{
    ASSERT_EQ(typeNode->GetType(), ENodeType::String);
    auto typeIndexString = typeNode->GetValue<TString>();
    auto typeIndex = FromString<int>(typeIndexString);
    ASSERT_LT(typeIndex, static_cast<int>(yqlTypes.size()));
    ASSERT_GE(typeIndex, 0);
    const auto& yqlType = yqlTypes[typeIndex];
    EXPECT_EQ(yqlType->GetType(), ENodeType::List);

    auto expectedTypeNode = [&] () -> INodePtr {
        using TDecayedType = std::decay_t<TType>;
        if constexpr (std::is_convertible_v<TDecayedType, TString>) {
            return ConvertToNode(TYsonString(TString(expectedType)));
        } else if constexpr (std::is_same_v<TDecayedType, INodePtr>) {
            return expectedType;
        } else {
            static_assert(TDependentFalse<TDecayedType>::value, "Type not allowed");
        }
    }();
    EXPECT_TRUE(AreNodesEqual(yqlType, expectedTypeNode))
        << "yqlType is " << ConvertToYsonString(yqlType, EYsonFormat::Pretty).GetData()
        << "\nexpectedTypeNode is " << ConvertToYsonString(expectedTypeNode, EYsonFormat::Pretty).GetData();
}

template <typename TValue, typename TType>
void CheckYqlTypeAndValue(
    const INodePtr& row,
    TStringBuf name,
    const TType& expectedType,
    const TValue& expectedValue,
    const std::vector<INodePtr>& yqlTypes)
{
    ASSERT_EQ(row->GetType(), ENodeType::Map);
    auto entry = row->AsMap()->FindChild(TString(name));
    ASSERT_TRUE(entry);
    ASSERT_EQ(entry->GetType(), ENodeType::List);
    ASSERT_EQ(entry->AsList()->GetChildCount(), 2);
    auto valueNode = entry->AsList()->GetChild(0);
    CheckYqlValue(valueNode, expectedValue);
    auto typeNode = entry->AsList()->GetChild(1);
    CheckYqlType(typeNode, expectedType, yqlTypes);
}

TEST_F(TWriterForWebJson, YqlValueFormat_SimpleTypes)
{
    Config_->MaxAllColumnNamesCount = 2;
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    // We will emulate writing rows from two tables.
    CreateStandardWriter({TTableSchema(), TTableSchema()});

    {
        TUnversionedOwningRowBuilder builder;
        std::vector<TUnversionedOwningRow> rows;

        builder.AddValue(MakeUnversionedUint64Value(100500, KeyAId_));
        builder.AddValue(MakeUnversionedBooleanValue(true, KeyBId_));
        builder.AddValue(MakeUnversionedStringValue("row1_c", KeyCId_));
        builder.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));
        builder.AddValue(MakeUnversionedInt64Value(0, TableIndexColumnId_));
        rows.push_back(builder.FinishRow());

        // Here come rows from the second table.

        builder.AddValue(MakeUnversionedStringValue("row2_c", KeyCId_));
        builder.AddValue(MakeUnversionedStringValue("row2_b", KeyBId_));
        builder.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));
        builder.AddValue(MakeUnversionedInt64Value(1, TableIndexColumnId_));
        rows.push_back(builder.FinishRow());

        builder.AddValue(MakeUnversionedInt64Value(-100500, KeyAId_));
        builder.AddValue(MakeUnversionedAnyValue("{x=2;y=3}", KeyBId_));
        builder.AddValue(MakeUnversionedDoubleValue(2.71828, KeyCId_));
        builder.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        std::vector<TUnversionedRow> nonOwningRows(rows.begin(), rows.end());
        EXPECT_EQ(true, Writer_->Write(nonOwningRows));
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto incompleteColumns = result->AsMap()->FindChild("incomplete_columns");
    ASSERT_TRUE(incompleteColumns);
    auto incompleteAllColumnNames = result->AsMap()->FindChild("incomplete_all_column_names");
    ASSERT_TRUE(incompleteAllColumnNames);
    auto allColumnNames = result->AsMap()->FindChild("all_column_names");
    ASSERT_TRUE(allColumnNames);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(incompleteColumns->GetType(), ENodeType::Boolean);
    EXPECT_EQ(incompleteColumns->GetValue<bool>(), false);

    ASSERT_EQ(incompleteAllColumnNames->GetType(), ENodeType::Boolean);
    EXPECT_EQ(incompleteAllColumnNames->GetValue<bool>(), true);

    ASSERT_EQ(allColumnNames->GetType(), ENodeType::List);
    std::vector<TString> allColumnNamesVector;
    ASSERT_NO_THROW(allColumnNamesVector = ConvertTo<decltype(allColumnNamesVector)>(allColumnNames));
    EXPECT_EQ(allColumnNamesVector, (std::vector<TString>{"column_a", "column_b"}));

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 3);

    auto row1 = rows->AsList()->GetChild(0);
    auto row2 = rows->AsList()->GetChild(1);
    auto row3 = rows->AsList()->GetChild(2);

    ASSERT_EQ(row1->GetType(), ENodeType::Map);
    EXPECT_EQ(row1->AsMap()->GetChildCount(), 3);
    CheckYqlTypeAndValue(row1, "column_a", R"(["DataType"; "Uint64"])", "100500", yqlTypes);
    CheckYqlTypeAndValue(row1, "column_b", R"(["DataType"; "Boolean"])", true, yqlTypes);
    CheckYqlTypeAndValue(row1, "column_c", R"(["DataType"; "String"])", "row1_c", yqlTypes);

    ASSERT_EQ(row2->GetType(), ENodeType::Map);
    EXPECT_EQ(row2->AsMap()->GetChildCount(), 2);
    CheckYqlTypeAndValue(row2, "column_b", R"(["DataType"; "String"])", "row2_b", yqlTypes);
    CheckYqlTypeAndValue(row2, "column_c", R"(["DataType"; "String"])", "row2_c", yqlTypes);

    ASSERT_EQ(row3->GetType(), ENodeType::Map);
    EXPECT_EQ(row3->AsMap()->GetChildCount(), 3);
    CheckYqlTypeAndValue(row3, "column_a", R"(["DataType"; "Int64"])", "-100500", yqlTypes);
    CheckYqlTypeAndValue(row3, "column_b", R"(["DataType"; "Yson"])", ConvertToNode(TYsonString("{x=2;y=3}")), yqlTypes);
    CheckYqlTypeAndValue(row3, "column_c", R"(["DataType"; "Double"])", 2.71828, yqlTypes);
}

TEST_F(TWriterForWebJson, YqlValueFormat_ComplexTypes)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;

    auto firstSchema = TTableSchema({
        {"column_a", OptionalLogicalType(
            ListLogicalType(MakeLogicalType(ESimpleLogicalValueType::Int64, true)))},
        {"column_b", StructLogicalType({
            {"key", MakeLogicalType(ESimpleLogicalValueType::String, true)},
            {"value", MakeLogicalType(ESimpleLogicalValueType::String, true)},
            {"variant_tuple", VariantTupleLogicalType({
                MakeLogicalType(ESimpleLogicalValueType::Int8, true),
                MakeLogicalType(ESimpleLogicalValueType::Boolean, false),
            })},
            {"variant_struct", VariantStructLogicalType({
                {"a", MakeLogicalType(ESimpleLogicalValueType::Int8, true)},
                {"b", MakeLogicalType(ESimpleLogicalValueType::Boolean, false)},
            })},
            {"dict", DictLogicalType(
                SimpleLogicalType(ESimpleLogicalValueType::Int64),
                SimpleLogicalType(ESimpleLogicalValueType::String)
            )},
            {"tagged", TaggedLogicalType(
                "MyTag",
                SimpleLogicalType(ESimpleLogicalValueType::Int64)
            )},
            {"timestamp", SimpleLogicalType(ESimpleLogicalValueType::Timestamp)},
            {"date", SimpleLogicalType(ESimpleLogicalValueType::Date)},
            {"datetime", SimpleLogicalType(ESimpleLogicalValueType::Datetime)},
            {"interval", SimpleLogicalType(ESimpleLogicalValueType::Interval)},
        })},
        {"column_c", ListLogicalType(StructLogicalType({
            {"very_optional_key", OptionalLogicalType(MakeLogicalType(ESimpleLogicalValueType::String, false))},
            {"optional_value", MakeLogicalType(ESimpleLogicalValueType::String, false)},
        }))},
    });

    auto secondSchema = TTableSchema({
        {"column_a", VariantTupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Null),
            SimpleLogicalType(ESimpleLogicalValueType::Any),
        })},
        {"column_b", SimpleLogicalType(ESimpleLogicalValueType::Null)},
        {"column_c", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Null))},
        {"column_d", OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    });

    auto firstColumnAType = ConvertToNode(TYsonString(R"([
        "OptionalType";
        [
            "ListType";
            ["DataType"; "Int64"]
        ]
    ])"));
    auto fristColumnBType = ConvertToNode(TYsonString(R"([
        "StructType";
        [
            [
                "key";
                ["DataType"; "String"]
            ];
            [
                "value";
                ["DataType"; "String"]
            ];
            [
                "variant_tuple";
                [
                    "VariantType";
                    [
                        "TupleType";
                        [
                            ["DataType"; "Int8"];
                            [
                                "OptionalType";
                                ["DataType"; "Boolean"]
                            ]
                        ]
                    ]
                ]
            ];
            [
                "variant_struct";
                [
                    "VariantType";
                    [
                        "StructType";
                        [
                            [
                                "a";
                                ["DataType"; "Int8"]
                            ];
                            [
                                "b";
                                [
                                    "OptionalType";
                                    ["DataType"; "Boolean"]
                                ]
                            ]
                        ]
                    ]
                ]
            ];
            [
                "dict";
                [
                    "DictType";
                    ["DataType"; "Int64"];
                    ["DataType"; "String"]
                ]
            ];
            [
                "tagged";
                [
                    "TaggedType";
                    "MyTag";
                    ["DataType"; "Int64"]
                ]
            ];
            [
                "timestamp";
                ["DataType"; "Timestamp"]
            ];
            [
                "date";
                ["DataType"; "Date"]
            ];
            [
                "datetime";
                ["DataType"; "Datetime"]
            ];
            [
                "interval";
                ["DataType"; "Interval"]
            ]
        ]
    ])"));
    auto firstColumnCType = ConvertToNode(TYsonString(R"([
        "ListType";
        [
            "StructType";
            [
                [
                    "very_optional_key";
                    [
                        "OptionalType";
                        [
                            "OptionalType";
                            ["DataType"; "String"]
                        ]
                    ]
                ];
                [
                    "optional_value";
                    [
                        "OptionalType";
                        ["DataType"; "String"]
                    ]
                ]
            ]
        ]
    ])"));
    auto secondColumnAType = ConvertToNode(TYsonString(R"([
        "VariantType";
        [
            "TupleType";
            [
                ["NullType"];
                ["DataType"; "Yson"];
            ]
        ]
    ])"));
    auto secondColumnBType = ConvertToNode(TYsonString(R"(["NullType"])"));
    auto secondColumnCType = ConvertToNode(TYsonString(R"([
        "OptionalType";
        [
            "NullType";
        ]
    ])"));
    auto secondColumnDType = ConvertToNode(TYsonString(R"([
        "OptionalType";
        ["DataType"; "Int64"]
    ])"));

    CreateStandardWriter({firstSchema, secondSchema});

    // "column_d" is registered but present only in second schema.
    KeyDId_ = NameTable_->RegisterName("column_d");

    {
        TUnversionedOwningRowBuilder builder;
        std::vector<TUnversionedOwningRow> rows;

        builder.AddValue(MakeUnversionedAnyValue(R"([-1; -2; -5])", KeyAId_));
        builder.AddValue(MakeUnversionedAnyValue(
            R"([
                "key";
                "value";
                [0; 7];
                [1; #];
                [[1; "a"]; [2; "b"]];
                99;
                100u;
                101u;
                102u;
                103
            ])",
            KeyBId_));
        builder.AddValue(MakeUnversionedAnyValue(R"([[[#]; "value"]; [["key"]; #]])", KeyCId_));
        builder.AddValue(MakeUnversionedInt64Value(-49, KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(0, TableIndexColumnId_));
        builder.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        builder.AddValue(MakeUnversionedAnyValue(R"([0; -2; -5; 177])", KeyAId_));
        builder.AddValue(MakeUnversionedAnyValue(
            R"([
                "key1";
                "value1";
                [1; %false];
                [1; #];
                [];
                199;
                0u;
                1101u;
                1102u;
                1103
            ])",
            KeyBId_));
        builder.AddValue(MakeUnversionedAnyValue(R"([[#; #]; [["key1"]; #]])", KeyCId_));
        builder.AddValue(MakeUnversionedUint64Value(49, KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        builder.AddValue(MakeUnversionedAnyValue(R"([])", KeyAId_));
        builder.AddValue(MakeUnversionedAnyValue(
            R"([
                "key2";
                "value2";
                [0; 127];
                [1; %true];
                [[0; ""]];
                399;
                30u;
                3101u;
                3202u;
                3103
            ])",
            KeyBId_));
        builder.AddValue(MakeUnversionedAnyValue(R"([[["key"]; #]])", KeyCId_));
        builder.AddValue(MakeUnversionedStringValue("49", KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(2, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        builder.AddValue(MakeUnversionedNullValue(KeyAId_));
        // First string is valid UTF-8, the second one should be Base64 encoded.
        builder.AddValue(MakeUnversionedAnyValue(
            "["
                "\"\xC3\xBF\";"
                "\"\xFA\xFB\xFC\xFD\";"
            R"(
                [0; 127];
                [1; %true];
                [[-1; "-1"]; [0; ""]];
                499;
                40u;
                4101u;
                4202u;
                4103
            ])",
            KeyBId_));
        builder.AddValue(MakeUnversionedAnyValue(R"([])", KeyCId_));
        builder.AddValue(MakeUnversionedAnyValue("{x=49}", KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(3, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        // Here come rows from the second table.

        builder.AddValue(MakeUnversionedAnyValue(R"([0; #])", KeyAId_));
        builder.AddValue(MakeUnversionedNullValue(KeyBId_));
        builder.AddValue(MakeUnversionedNullValue(KeyCId_));
        builder.AddValue(MakeUnversionedInt64Value(-49, KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(1, TableIndexColumnId_));
        builder.AddValue(MakeUnversionedInt64Value(0, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        builder.AddValue(MakeUnversionedAnyValue(R"([1; {z=z}])", KeyAId_));
        builder.AddValue(MakeUnversionedNullValue(KeyBId_));
        builder.AddValue(MakeUnversionedAnyValue(R"([#])", KeyCId_));
        builder.AddValue(MakeUnversionedNullValue(KeyDId_));
        builder.AddValue(MakeUnversionedInt64Value(1, TableIndexColumnId_));
        builder.AddValue(MakeUnversionedInt64Value(1, RowIndexColumnId_));
        rows.push_back(builder.FinishRow());

        std::vector<TUnversionedRow> nonOwningRows(rows.begin(), rows.end());
        EXPECT_EQ(true, Writer_->Write(nonOwningRows));
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto incompleteColumns = result->AsMap()->FindChild("incomplete_columns");
    ASSERT_TRUE(incompleteColumns);
    auto incompleteAllColumnNames = result->AsMap()->FindChild("incomplete_all_column_names");
    ASSERT_TRUE(incompleteAllColumnNames);
    auto allColumnNames = result->AsMap()->FindChild("all_column_names");
    ASSERT_TRUE(allColumnNames);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(incompleteColumns->GetType(), ENodeType::Boolean);
    EXPECT_EQ(incompleteColumns->GetValue<bool>(), false);

    ASSERT_EQ(incompleteAllColumnNames->GetType(), ENodeType::Boolean);
    EXPECT_EQ(incompleteAllColumnNames->GetValue<bool>(), false);

    ASSERT_EQ(allColumnNames->GetType(), ENodeType::List);
    std::vector<TString> allColumnNamesVector;
    ASSERT_NO_THROW(allColumnNamesVector = ConvertTo<decltype(allColumnNamesVector)>(allColumnNames));
    EXPECT_EQ(allColumnNamesVector, (std::vector<TString>{"column_a", "column_b", "column_c", "column_d"}));

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 6);

    auto row1 = rows->AsList()->GetChild(0);
    auto row2 = rows->AsList()->GetChild(1);
    auto row3 = rows->AsList()->GetChild(2);
    auto row4 = rows->AsList()->GetChild(3);
    auto row5 = rows->AsList()->GetChild(4);
    auto row6 = rows->AsList()->GetChild(5);

    ASSERT_EQ(row1->GetType(), ENodeType::Map);
    EXPECT_EQ(row1->AsMap()->GetChildCount(), 4);
    auto row1AValue = ConvertToNode(TYsonString(R"([{"val"=["-1"; "-2"; "-5"]}])"));
    CheckYqlTypeAndValue(row1, "column_a", firstColumnAType, row1AValue, yqlTypes);
    auto row1BValue = ConvertToNode(TYsonString(
        R"([
            "key";
            "value";
            ["0"; "7"];
            ["1"; #];
            {"val"=[["1"; "a"]; ["2"; "b"]]};
            "99";
            "100";
            "101";
            "102";
            "103"
        ])"));
    CheckYqlTypeAndValue(row1, "column_b", fristColumnBType, row1BValue, yqlTypes);
    auto row1CValue = ConvertToNode(TYsonString(R"({
        "val"=[
            [[#]; ["value"]];
            [[["key"]]; #]
        ]
    })"));
    CheckYqlTypeAndValue(row1, "column_c", firstColumnCType, row1CValue, yqlTypes);
    CheckYqlTypeAndValue(row1, "column_d", R"(["DataType"; "Int64"])", "-49", yqlTypes);

    ASSERT_EQ(row2->GetType(), ENodeType::Map);
    EXPECT_EQ(row2->AsMap()->GetChildCount(), 4);
    auto row2AValue = ConvertToNode(TYsonString(R"([{"val"=["0"; "-2"; "-5"; "177"]}])"));
    CheckYqlTypeAndValue(row2, "column_a", firstColumnAType, row2AValue, yqlTypes);
    auto row2BValue = ConvertToNode(TYsonString(
        R"([
            "key1";
            "value1";
            ["1"; [%false]];
            ["1"; #];
            {"val"=[]};
            "199";
            "0";
            "1101";
            "1102";
            "1103"
        ])"));
    CheckYqlTypeAndValue(row2, "column_b", fristColumnBType, row2BValue, yqlTypes);
    auto row2CValue = ConvertToNode(TYsonString(R"({
        "val"=[
            [#; #];
            [[["key1"]]; #]
        ]
    })"));
    CheckYqlTypeAndValue(row2, "column_c", firstColumnCType, row2CValue, yqlTypes);
    CheckYqlTypeAndValue(row2, "column_d", R"(["DataType"; "Uint64"])", "49", yqlTypes);

    ASSERT_EQ(row3->GetType(), ENodeType::Map);
    EXPECT_EQ(row3->AsMap()->GetChildCount(), 4);
    auto row3AValue = ConvertToNode(TYsonString(R"([{"val"=[]}])"));
    CheckYqlTypeAndValue(row3, "column_a", firstColumnAType, row3AValue, yqlTypes);
    auto row3BValue = ConvertToNode(TYsonString(
        R"([
            "key2";
            "value2";
            ["0"; "127"];
            ["1"; [%true]];
            {"val"=[["0"; ""]]};
            "399";
            "30";
            "3101";
            "3202";
            "3103"
        ])"));
    CheckYqlTypeAndValue(row3, "column_b", fristColumnBType, row3BValue, yqlTypes);
    auto row3CValue = ConvertToNode(TYsonString(R"({
        "val"=[
            [[["key"]]; #]
        ]
    })"));
    CheckYqlTypeAndValue(row3, "column_c", firstColumnCType, row3CValue, yqlTypes);
    CheckYqlTypeAndValue(row3, "column_d", R"(["DataType"; "String"])", "49", yqlTypes);

    ASSERT_EQ(row4->GetType(), ENodeType::Map);
    EXPECT_EQ(row4->AsMap()->GetChildCount(), 4);
    auto row4AValue = ConvertToNode(TYsonString(R"(#)"));
    CheckYqlTypeAndValue(row4, "column_a", firstColumnAType, row4AValue, yqlTypes);

    auto row4BValue = ConvertToNode(TYsonString(
        "["
            "\"\xC3\xBF\";"
        R"(
            {"b64" = %true; "val" = "+vv8/Q=="};
            ["0"; "127"];
            ["1"; [%true]];
            {"val"=[["-1"; "-1"]; ["0"; ""]]};
            "499";
            "40";
            "4101";
            "4202";
            "4103"
        ])"));
    CheckYqlTypeAndValue(row4, "column_b", fristColumnBType, row4BValue, yqlTypes);

    auto row4CValue = ConvertToNode(TYsonString(R"({"val"=[]})"));
    CheckYqlTypeAndValue(row4, "column_c", firstColumnCType, row4CValue, yqlTypes);
    auto row4DValue = ConvertToNode(TYsonString("{x=49}"));
    CheckYqlTypeAndValue(row4, "column_d", R"(["DataType"; "Yson"])", row4DValue, yqlTypes);

    // Here must come rows from the second table.

    ASSERT_EQ(row5->GetType(), ENodeType::Map);
    EXPECT_EQ(row5->AsMap()->GetChildCount(), 4);
    auto row5AValue = ConvertToNode(TYsonString(R"(["0"; #])"));
    CheckYqlTypeAndValue(row5, "column_a", secondColumnAType, row5AValue, yqlTypes);
    auto row5BValue = ConvertToNode(TYsonString(R"(#)"));
    CheckYqlTypeAndValue(row5, "column_b", secondColumnBType, row5BValue, yqlTypes);
    auto row5CValue = ConvertToNode(TYsonString(R"(#)"));
    CheckYqlTypeAndValue(row5, "column_c", secondColumnCType, row5CValue, yqlTypes);
    auto row5DValue = ConvertToNode(TYsonString(R"(["-49"])"));
    CheckYqlTypeAndValue(row5, "column_d", secondColumnDType, row5DValue, yqlTypes);

    ASSERT_EQ(row6->GetType(), ENodeType::Map);
    EXPECT_EQ(row6->AsMap()->GetChildCount(), 4);
    {
        auto entry = row6->AsMap()->FindChild("column_a");
        ASSERT_TRUE(entry);
        ASSERT_EQ(entry->GetType(), ENodeType::List);
        ASSERT_EQ(entry->AsList()->GetChildCount(), 2);
        auto valueNode = entry->AsList()->GetChild(0);
        ASSERT_EQ(valueNode->GetType(), ENodeType::List);
        ASSERT_EQ(valueNode->AsList()->GetChildCount(), 2);
        ASSERT_EQ(valueNode->AsList()->GetChild(0)->GetType(), ENodeType::String);
        EXPECT_EQ(valueNode->AsList()->GetChild(0)->GetValue<TString>(), "1");
        ASSERT_EQ(valueNode->AsList()->GetChild(1)->GetType(), ENodeType::String);
        EXPECT_TRUE(AreNodesEqual(
            ConvertToNode(TYsonString(valueNode->AsList()->GetChild(1)->GetValue<TString>())),
            ConvertToNode(TYsonString("{z=z}"))));
        auto typeNode = entry->AsList()->GetChild(1);
        CheckYqlType(typeNode, secondColumnAType, yqlTypes);
    }
    auto row6BValue = ConvertToNode(TYsonString(R"(#)"));
    CheckYqlTypeAndValue(row6, "column_b", secondColumnBType, row6BValue, yqlTypes);
    auto row6CValue = ConvertToNode(TYsonString(R"([#])"));
    CheckYqlTypeAndValue(row6, "column_c", secondColumnCType, row6CValue, yqlTypes);
    auto row6DValue = ConvertToNode(TYsonString(R"(#)"));
    CheckYqlTypeAndValue(row6, "column_d", secondColumnDType, row6DValue, yqlTypes);
}

TEST_F(TWriterForWebJson, YqlValueFormat_Incomplete)
{
    Config_->ValueFormat = EWebJsonValueFormat::Yql;
    Config_->FieldWeightLimit = 215;
    Config_->StringWeightLimit = 10;

    auto schema = TTableSchema({
        {"column_a", StructLogicalType({
            {"field1", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"list", ListLogicalType(
                VariantStructLogicalType({
                    {"a", DictLogicalType(
                        SimpleLogicalType(ESimpleLogicalValueType::Int64),
                        SimpleLogicalType(ESimpleLogicalValueType::String)
                    )},
                    {"b", SimpleLogicalType(ESimpleLogicalValueType::Any)},
                })
            )},
            {"field2", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"field3", MakeLogicalType(ESimpleLogicalValueType::Int64, false)},
        })},
        {"column_b", SimpleLogicalType(ESimpleLogicalValueType::Any)},
        {"column_c", MakeLogicalType(ESimpleLogicalValueType::String, false)},
    });

    auto yqlTypeA = ConvertToNode(TYsonString(R"([
        "StructType";
        [
            [
                "field1";
                ["DataType"; "Int64"]
            ];
            [
                "list";
                [
                    "ListType";
                    [
                        "VariantType";
                        [
                            "StructType";
                            [
                                [
                                    "a";
                                    [
                                        "DictType";
                                        ["DataType"; "Int64"];
                                        ["DataType"; "String"]
                                    ]
                                ];
                                [
                                    "b";
                                    ["DataType"; "Yson"]
                                ];
                            ]
                        ]
                    ]
                ]
            ];
            [
                "field2";
                ["DataType"; "String"]
            ];
            [
                "field3";
                [
                    "OptionalType";
                    ["DataType"; "Int64"]
                ]
            ];
        ]
    ])"));

    auto yqlTypeB = ConvertToNode(TYsonString(R"(["DataType"; "Yson"])"));
    auto yqlTypeC = ConvertToNode(TYsonString(R"(["OptionalType"; ["DataType"; "String"]])"));
    {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedAnyValue(R"([
            -1;
            [
                [
                    0;
                    [
                        [-2; "UTF:)" + TString("\xF0\x90\x8D\x88") + "\xF0\x90\x8D\x88" + R"("];
                        [2; "!UTF:)" + TString("\xFA\xFB\xFC\xFD\xFA\xFB\xFC\xFD") + R"("];
                        [0; ""];
                    ]
                ];
                [
                    1;
                    "{kinda_long_key = kinda_even_longer_value}"
                ];
                [
                    0;
                    [
                        [0; "One more quite long string"];
                        [1; "One more quite long string"];
                        [2; "One more quite long string"];
                        [3; "One more quite long string"];
                        [4; "One more quite long string"];
                        [5; "One more quite long string"];
                    ]
                ];
                [
                    1;
                    "{kinda_long_key = kinda_even_longer_value}"
                ];
            ];
            "I'm short";
            424242238133245
        ])", KeyAId_));
        builder.AddValue(MakeUnversionedAnyValue("{kinda_long_key = kinda_even_longer_value}", KeyBId_));
        builder.AddValue(MakeUnversionedStringValue("One more quite long string", KeyCId_));
        CreateStandardWriter({schema});
        EXPECT_EQ(true, Writer_->Write({builder.FinishRow()}));
        Writer_->Close().Get().ThrowOnError();
    }

    auto result = ParseJsonToNode(OutputStream_.Str());
    ASSERT_EQ(result->GetType(), ENodeType::Map);

    auto rows = result->AsMap()->FindChild("rows");
    ASSERT_TRUE(rows);
    auto yqlTypeRegistry = result->AsMap()->FindChild("yql_type_registry");
    ASSERT_TRUE(yqlTypeRegistry);

    ASSERT_EQ(yqlTypeRegistry->GetType(), ENodeType::List);
    auto yqlTypes = ConvertTo<std::vector<INodePtr>>(yqlTypeRegistry);

    ASSERT_EQ(rows->GetType(), ENodeType::List);
    ASSERT_EQ(rows->AsList()->GetChildCount(), 1);

    auto row = rows->AsList()->GetChild(0);
    ASSERT_EQ(row->GetType(), ENodeType::Map);
    EXPECT_EQ(row->AsMap()->GetChildCount(), 3);

    auto rowAValue = ConvertToNode(TYsonString(R"([
        "-1";
        {
            "inc" = %true;
            "val" = [
                [
                    "0";
                    {
                        "val" = [
                            ["-2"; {"inc"=%true; "val"="UTF:)" + TString("\xF0\x90\x8D\x88") + R"("}];
                            ["2"; {"inc"=%true; "b64"=%true; "val"="IVVURjr6"}];
                            ["0"; ""];
                        ]
                    }
                ];
                [
                    "1";
                    {"val"=""; "inc"=%true}
                ];
                [
                    "0";
                    {
                        "inc" = %true;
                        "val" = [
                            ["0"; {"val"="One more q"; "inc"=%true}];
                            ["1"; {"val"="One more"; "inc"=%true}];
                        ];
                    }
                ];
            ];
        };
        {
            "val" = "";
            "inc" = %true;
        };
        ["424242238133245"];
    ])"));
    CheckYqlTypeAndValue(row, "column_a", yqlTypeA, rowAValue, yqlTypes);

    // Simple values are not truncated to |StringWeightLimit|
    auto rowBValue = ConvertToNode(TYsonString(R"({kinda_long_key = kinda_even_longer_value})"));
    CheckYqlTypeAndValue(row, "column_b", yqlTypeB, rowBValue, yqlTypes);
    auto rowCValue = ConvertToNode(TYsonString(R"(["One more quite long string"])"));
    CheckYqlTypeAndValue(row, "column_c", yqlTypeC, rowCValue, yqlTypes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats
