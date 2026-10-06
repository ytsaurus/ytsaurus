#include <yt/yt/core/test_framework/framework.h>

#include "value_examples.h"
#include "yson_helpers.h"

#include <yt/yt/tests/cpp/library/row_helpers.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/validate_logical_type.h>

#include <yt/yt/library/formats/format.h>
#include <yt/yt/library/formats/skiff_parser.h>
#include <yt/yt/library/formats/skiff_writer.h>

#include <yt/yt/library/logical_type_shortcuts/logical_type_shortcuts.h>

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/library/tz_types/tz_types.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/async_stream_helpers.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <library/cpp/yt/string/stream.h>

#include <util/generic/hash.h>
#include <util/generic/ylimits.h>

#include <util/stream/mem.h>

#include <util/string/hex.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <functional>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace NYT {
namespace {

using namespace NConcurrency;
using namespace NFormats;
using namespace NNamedValue;
using namespace NSkiff;
using namespace NSkiffExt;
using namespace NTableClient;
using namespace NLogicalTypeShortcuts;
using namespace NTzTypes;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using TNamedRow = std::vector<TNamedValue>;
using TNamedRows = std::vector<TNamedRow>;

struct TSkiffWriterOptions
{
    int KeyColumnCount = 0;
    bool EnableKeySwitch = false;
    bool EnableEndOfStream = false;
};

TTableSchemaPtr MakeTableSchema(std::vector<TColumnSchema> columns)
{
    return New<TTableSchema>(std::move(columns));
}

TSkiffSchemaPtr CreateOptionalSchema(TSkiffSchemaPtr schema)
{
    return CreateVariant8Schema({
        CreateSimpleTypeSchema(EWireType::Nothing),
        std::move(schema),
    });
}

TSkiffSchemaPtr CreateOptionalSchema(EWireType wireType)
{
    return CreateOptionalSchema(CreateSimpleTypeSchema(wireType));
}

TSkiffSchemaPtr CreateSparseColumnsSchema(TSkiffSchemaList children)
{
    return CreateRepeatedVariant16Schema(std::move(children))->SetName(TString(SparseColumnsName));
}

std::string ToBinaryYson(TStringBuf yson)
{
    auto node = ConvertToNode(TYsonString(yson));
    return std::string(ConvertToYsonString(node, EYsonFormat::Binary).AsStringBuf());
}

std::string WriteSkiff(
    const TSkiffSchemaPtr& skiffSchema,
    const TNamedRows& rows,
    const TTableSchemaPtr& tableSchema,
    TNameTablePtr nameTable,
    const TSkiffWriterOptions& writerOptions = {})
{
    if (!nameTable) {
        nameTable = New<TNameTable>();
    }
    auto controlAttributesConfig = New<TControlAttributesConfig>();
    controlAttributesConfig->EnableKeySwitch = writerOptions.EnableKeySwitch;
    controlAttributesConfig->EnableEndOfStream = writerOptions.EnableEndOfStream;

    TStdStringStream output;
    auto writer = CreateWriterForSkiff(
        {skiffSchema},
        nameTable,
        {tableSchema},
        CreateAsyncAdapter(&output),
        /*enableContextSaving*/ false,
        controlAttributesConfig,
        writerOptions.KeyColumnCount);
    for (const auto& row : rows) {
        auto owningRow = MakeRow(nameTable, row);
        if (!writer->Write({owningRow.Get()})) {
            WaitForFast(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }
    WaitForFast(writer->Close())
        .ThrowOnError();
    return output.Str();
}

// With |endOfStream|, |write| must end with an end-of-sequence tag, as the writer does when
// $end_of_stream is enabled.
std::string MakeSkiffData(
    const TSkiffSchemaPtr& skiffSchema,
    const std::function<void(TCheckedSkiffWriter*)>& write,
    bool endOfStream = false)
{
    TStdStringStream output;
    auto streamSchema = endOfStream
        ? TSkiffSchemaPtr(CreateRepeatedVariant16Schema({skiffSchema}))
        : TSkiffSchemaPtr(CreateVariant16Schema({skiffSchema}));
    TCheckedSkiffWriter writer(streamSchema, &output);
    write(&writer);
    writer.Finish();
    return output.Str();
}

template <class T>
void WriteInteger(TCheckedSkiffWriter* writer, EWireType wireType, T value)
{
    switch (wireType) {
        case EWireType::Int8:
            writer->WriteInt8(static_cast<i8>(value));
            break;
        case EWireType::Int16:
            writer->WriteInt16(static_cast<i16>(value));
            break;
        case EWireType::Int32:
            writer->WriteInt32(static_cast<i32>(value));
            break;
        case EWireType::Int64:
            writer->WriteInt64(static_cast<i64>(value));
            break;
        case EWireType::Uint8:
            writer->WriteUint8(static_cast<ui8>(value));
            break;
        case EWireType::Uint16:
            writer->WriteUint16(static_cast<ui16>(value));
            break;
        case EWireType::Uint32:
            writer->WriteUint32(static_cast<ui32>(value));
            break;
        case EWireType::Uint64:
            writer->WriteUint64(static_cast<ui64>(value));
            break;
        default:
            YT_ABORT();
    }
}

TNamedRow CanonizeRow(const TNameTablePtr& nameTable, TUnversionedRow row)
{
    std::vector<std::pair<std::string, TNamedValue::TValue>> values;
    for (const auto& value : row) {
        auto extracted = TNamedValue::ExtractValue(value);
        if (auto* any = std::get_if<TNamedValue::TAny>(&extracted)) {
            any->Value = CanonizeYson(any->Value);
        } else if (auto* composite = std::get_if<TNamedValue::TComposite>(&extracted)) {
            composite->Value = CanonizeYson(composite->Value);
        }
        values.emplace_back(nameTable->GetName(value.Id), std::move(extracted));
    }
    std::sort(values.begin(), values.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    TNamedRow result;
    for (auto& [name, value] : values) {
        result.emplace_back(std::move(name), std::move(value));
    }
    return result;
}

TNamedRows CanonizeRows(const TNamedRows& rows)
{
    auto nameTable = New<TNameTable>();
    TNamedRows result;
    for (const auto& row : rows) {
        result.push_back(CanonizeRow(nameTable, MakeRow(nameTable, row)));
    }
    return result;
}

TNamedRows ParseSkiff(
    const TSkiffSchemaPtr& skiffSchema,
    TStringBuf data,
    const TTableSchemaPtr& tableSchema,
    TNameTablePtr nameTable = nullptr)
{
    if (!nameTable) {
        nameTable = New<TNameTable>();
    }
    TCollectingValueConsumer rowCollector(nameTable, tableSchema);
    auto parser = CreateParserForSkiff(skiffSchema, &rowCollector);
    parser->Read(data);
    parser->Finish();

    TNamedRows result;
    for (const auto& row : rowCollector.GetRowList()) {
        result.push_back(CanonizeRow(nameTable, row));
    }
    return result;
}

template <class T>
std::string MakeValueCaseName(T value)
{
    auto result = std::string(ToString(value));
    if (result.starts_with('-')) {
        result = "minus" + result.substr(1);
    }
    return result;
}

// GoogleTest accepts only alphanumerics and underscores in a case name;
// `Decimal(3,2)` would be rejected.
std::string MakeTypeCaseName(const TLogicalType& logicalType)
{
    // Format prints `Yson()` under its enum name, `Any`.
    if (logicalType == *Yson()) {
        return "yson";
    }
    if (logicalType.GetMetatype() == ELogicalMetatype::Optional) {
        return "optional_" + MakeTypeCaseName(*logicalType.AsOptionalTypeRef().GetElement());
    }

    std::string result;
    for (unsigned char character : Format("%v", logicalType)) {
        if (std::isupper(character)) {
            if (!result.empty() && result.back() != '_') {
                result += '_';
            }
            result += static_cast<char>(std::tolower(character));
        } else if (std::isalnum(character)) {
            result += static_cast<char>(character);
        } else if (!result.empty() && result.back() != '_') {
            result += '_';
        }
    }
    while (result.ends_with('_')) {
        result.pop_back();
    }
    return result;
}

template <class TCase>
std::string GetCaseName(const ::testing::TestParamInfo<TCase>& info)
{
    return info.param.CaseName;
}

struct TExpectedData
{
    std::string Hex;
};

TExpectedData MakeExpectedData(TStringBuf data)
{
    return {.Hex = std::string(HexEncode(data))};
}

// For a single row holding a single Yson32 column: compare the written YSON canonically instead
// of bytewise.
struct TExpectedYson
{
    std::string Text;
};

struct TExpectedError
{
    std::string Substring;
};

////////////////////////////////////////////////////////////////////////////////
// Round trip: rows --writer--> bytes --parser--> rows.

struct TRoundTripCase
{
    std::string CaseName;
    TSkiffSchemaPtr SkiffSchema;
    TTableSchemaPtr TableSchema = New<TTableSchema>();
    TNamedRows Rows;
    std::variant<TExpectedData, TExpectedYson> Expected;
    TNameTablePtr NameTable;
};

class TSkiffRoundTripTest
    : public ::testing::TestWithParam<TRoundTripCase>
{ };

TEST_P(TSkiffRoundTripTest, RoundTrip)
{
    const auto& testCase = GetParam();
    auto data = WriteSkiff(testCase.SkiffSchema, testCase.Rows, testCase.TableSchema, testCase.NameTable);
    if (const auto* expectedData = std::get_if<TExpectedData>(&testCase.Expected)) {
        EXPECT_EQ(HexEncode(data), expectedData->Hex);
    } else {
        TMemoryInput input(data);
        TCheckedSkiffParser parser(CreateVariant16Schema({testCase.SkiffSchema}), &input);
        ASSERT_EQ(parser.ParseVariant16Tag(), 0);
        EXPECT_EQ(
            CanonizeYson(parser.ParseYson32()),
            CanonizeYson(std::get<TExpectedYson>(testCase.Expected).Text));
        parser.ValidateFinished();
    }

    EXPECT_EQ(
        ParseSkiff(testCase.SkiffSchema, data, testCase.TableSchema, testCase.NameTable),
        CanonizeRows(testCase.Rows));
}

// Collects cases for a table with a single column named "column"; the row tag is added here.
struct TValueCaseBuilder
{
    using TWriteValue = std::function<void(TCheckedSkiffWriter*)>;

    std::vector<TRoundTripCase> Cases;

    void Add(
        const std::string& name,
        const TSkiffSchemaPtr& fieldSchema,
        const TLogicalTypePtr& logicalType,
        const TNamedValue::TValue& value,
        const TWriteValue& writeValue)
    {
        auto skiffSchema = CreateTupleSchema({fieldSchema->SetName("column")});
        Cases.push_back({
            .CaseName = name,
            .SkiffSchema = skiffSchema,
            .TableSchema = logicalType ? MakeTableSchema({{"column", logicalType}}) : New<TTableSchema>(),
            .Rows = {{{"column", value}}},
            .Expected = MakeExpectedData(MakeSkiffData(skiffSchema, [&] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant16Tag(0);
                writeValue(writer);
            })),
        });
    }

    void Add(
        const std::string& name,
        const TSkiffSchemaPtr& fieldSchema,
        const TNamedValue::TValue& value,
        const TWriteValue& writeValue)
    {
        Add(name, fieldSchema, /*logicalType*/ nullptr, value, writeValue);
    }

    template <class T>
    void AddSimpleAndList(
        const std::string& name,
        EWireType wireType,
        const TLogicalTypePtr& logicalType,
        T value,
        const TWriteValue& writeValue)
    {
        Add(name, CreateSimpleTypeSchema(wireType), logicalType, value, writeValue);
        auto listValue = TNamedValue::TComposite{
            BuildYsonStringFluently().BeginList().Item().Value(value).EndList().ToString(),
        };
        Add(
            "list_" + name,
            CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(wireType)}),
            List(logicalType),
            listValue,
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant8Tag(0);
                writeValue(writer);
                writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
            });
    }
};

std::vector<TRoundTripCase> MakeWireTypeCases()
{
    TValueCaseBuilder builder;
    auto addSchemafulAndSchemaless = [&] (
        const std::string& name,
        const TSkiffSchemaPtr& fieldSchema,
        const TLogicalTypePtr& logicalType,
        const TNamedValue::TValue& value,
        const TValueCaseBuilder::TWriteValue& writeValue)
    {
        builder.Add(name + "_schemaful", fieldSchema, logicalType, value, writeValue);
        builder.Add(name + "_schemaless", fieldSchema, value, writeValue);
    };
    auto add = [&] (
        const std::string& name,
        const TSkiffSchemaPtr& fieldSchema,
        const TLogicalTypePtr& logicalType,
        const TNamedValue::TValue& value,
        const TValueCaseBuilder::TWriteValue& writeValue)
    {
        addSchemafulAndSchemaless(name, fieldSchema, logicalType, value, writeValue);
        addSchemafulAndSchemaless(
            "optional_" + name,
            CreateOptionalSchema(fieldSchema),
            Optional(logicalType),
            value,
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant8Tag(1);
                writeValue(writer);
            });
        addSchemafulAndSchemaless(
            "optional_" + name + "_null",
            CreateOptionalSchema(fieldSchema),
            Optional(logicalType),
            /*value*/ nullptr,
            [] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant8Tag(0);
            });
    };

    addSchemafulAndSchemaless(
        "nothing",
        CreateSimpleTypeSchema(EWireType::Nothing),
        Null(),
        /*value*/ nullptr,
        [] (TCheckedSkiffWriter* /*writer*/) { });
    add(
        "boolean",
        CreateSimpleTypeSchema(EWireType::Boolean),
        Bool(),
        /*value*/ true,
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteBoolean(true);
        });
    add(
        "int64",
        CreateSimpleTypeSchema(EWireType::Int64),
        Int64(),
        /*value*/ -1,
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteInt64(-1);
        });
    add(
        "uint64",
        CreateSimpleTypeSchema(EWireType::Uint64),
        Uint64(),
        /*value*/ 2ull,
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteUint64(2);
        });
    add(
        "double",
        CreateSimpleTypeSchema(EWireType::Double),
        Double(),
        /*value*/ 3.0,
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteDouble(3.0);
        });
    add(
        "string32",
        CreateSimpleTypeSchema(EWireType::String32),
        String(),
        /*value*/ "four",
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteString32("four");
        });

    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    WireTypes,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeWireTypeCases()),
    GetCaseName<TRoundTripCase>);

constexpr auto SignedSmallIntLimits = std::to_array<std::tuple<EWireType, i64, i64>>({
    {EWireType::Int8, Min<i8>(), Max<i8>()},
    {EWireType::Int16, Min<i16>(), Max<i16>()},
    {EWireType::Int32, Min<i32>(), Max<i32>()},
});

constexpr auto UnsignedSmallIntLimits = std::to_array<std::pair<EWireType, ui64>>({
    {EWireType::Uint8, Max<ui8>()},
    {EWireType::Uint16, Max<ui16>()},
    {EWireType::Uint32, Max<ui32>()},
});

std::vector<TRoundTripCase> MakeSmallIntCases()
{
    TValueCaseBuilder builder;
    auto addInt = [&] (EWireType wireType, auto value) {
        auto suffix = MakeValueCaseName(value) + "_as_" + std::string(ToString(wireType));
        auto writeValue = [&] (TCheckedSkiffWriter* writer) {
            WriteInteger(writer, wireType, value);
        };
        builder.Add(
            MakeTypeCaseName(*Yson()) + "_" + suffix,
            CreateSimpleTypeSchema(wireType),
            Yson(),
            value,
            writeValue);

        std::vector<TLogicalTypePtr> logicalTypes;
        if constexpr (std::is_signed_v<decltype(value)>) {
            if (std::in_range<i8>(value)) {
                logicalTypes.push_back(Int8());
            }
            if (std::in_range<i16>(value)) {
                logicalTypes.push_back(Int16());
            }
            if (std::in_range<i32>(value)) {
                logicalTypes.push_back(Int32());
            }
            logicalTypes.push_back(Int64());
        } else {
            if (std::in_range<ui8>(value)) {
                logicalTypes.push_back(Uint8());
            }
            if (std::in_range<ui16>(value)) {
                logicalTypes.push_back(Uint16());
            }
            if (std::in_range<ui32>(value)) {
                logicalTypes.push_back(Uint32());
            }
            logicalTypes.push_back(Uint64());
        }
        for (const auto& logicalType : logicalTypes) {
            builder.AddSimpleAndList(
                MakeTypeCaseName(*logicalType) + "_" + suffix,
                wireType,
                logicalType,
                value,
                writeValue);
        }
    };

    for (auto [wireType, minValue, maxValue] : SignedSmallIntLimits) {
        for (i64 value : std::initializer_list<i64>{0, 42, -42, maxValue, minValue}) {
            addInt(wireType, value);
        }
    }
    for (auto [wireType, maxValue] : UnsignedSmallIntLimits) {
        for (ui64 value : std::initializer_list<ui64>{0, 42, maxValue}) {
            addInt(wireType, value);
        }
    }

    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    SmallInts,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeSmallIntCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeFloatCases()
{
    TValueCaseBuilder builder;
    builder.AddSimpleAndList(
        "float_as_double",
        EWireType::Double,
        Float(),
        /*value*/ 3.0,
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteDouble(3.0);
        });
    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    Float,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeFloatCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeDateTimeTypeCases()
{
    TValueCaseBuilder builder;
    auto add = [&] (
        EWireType wireType,
        const TLogicalTypePtr& logicalType,
        auto value)
    {
        builder.AddSimpleAndList(
            MakeTypeCaseName(*logicalType) + "_" + MakeValueCaseName(value) + "_as_" + std::string(ToString(wireType)),
            wireType,
            logicalType,
            value,
            [&] (TCheckedSkiffWriter* writer) {
                WriteInteger(writer, wireType, value);
            });
    };

    auto unsignedTypeLimits = std::to_array<std::tuple<EWireType, TLogicalTypePtr, ui64>>({
        {EWireType::Uint16, Date(), DateUpperBound - 1},
        {EWireType::Uint32, Datetime(), DatetimeUpperBound - 1},
        {EWireType::Uint64, Timestamp(), TimestampUpperBound - 1},
    });
    for (const auto& [wireType, logicalType, maxValue] : unsignedTypeLimits) {
        for (ui64 value : std::initializer_list<ui64>{0, 42, maxValue}) {
            add(wireType, logicalType, value);
        }
    }

    auto intervalMax = static_cast<i64>(TimestampUpperBound) - 1;
    auto signedTypeLimits = std::to_array<std::tuple<EWireType, TLogicalTypePtr, i64, i64>>({
        {EWireType::Int32, Date32(), Date32LowerBound, Date32UpperBound - 1},
        {EWireType::Int64, Date32(), Date32LowerBound, Date32UpperBound - 1},
        {EWireType::Int64, Datetime64(), Datetime64LowerBound, Datetime64UpperBound - 1},
        {EWireType::Int64, Timestamp64(), Timestamp64LowerBound, Timestamp64UpperBound - 1},
        {EWireType::Int64, Interval64(), -Interval64UpperBound + 1, Interval64UpperBound - 1},
        {EWireType::Int64, Interval(), -intervalMax, intervalMax},
    });
    for (const auto& [wireType, logicalType, minValue, maxValue] : signedTypeLimits) {
        for (i64 value : std::initializer_list<i64>{0, 42, maxValue, minValue}) {
            add(wireType, logicalType, value);
        }
    }

    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    DateTimeTypes,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeDateTimeTypeCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeTzTypeCases()
{
    TValueCaseBuilder builder;
    auto add = [&] (
        const std::string& name,
        EWireType wireType,
        const TLogicalTypePtr& logicalType,
        auto value,
        ui16 tzId)
    {
        builder.Add(
            name,
            CreateTupleSchema({CreateSimpleTypeSchema(wireType), CreateSimpleTypeSchema(EWireType::Uint16)}),
            logicalType,
            MakeTzString(value, tzId),
            [&] (TCheckedSkiffWriter* writer) {
                WriteInteger(writer, wireType, value);
                writer->WriteUint16(tzId);
            });
    };

    add("tz_date", EWireType::Uint16, TzDate(), static_cast<ui16>(DateUpperBound - 1), /*tzId*/ 1);
    add("tz_datetime", EWireType::Uint32, TzDatetime(), static_cast<ui32>(DatetimeUpperBound - 1), /*tzId*/ 2);
    add("tz_timestamp", EWireType::Uint64, TzTimestamp(), TimestampUpperBound - 1, /*tzId*/ 3);
    add("tz_date32", EWireType::Int32, TzDate32(), static_cast<i32>(Date32LowerBound), /*tzId*/ 1);
    add("tz_datetime64", EWireType::Int64, TzDatetime64(), Datetime64LowerBound, /*tzId*/ 2);
    add("tz_timestamp64", EWireType::Int64, TzTimestamp64(), Timestamp64LowerBound, /*tzId*/ 3);

    auto tzDate = MakeTzString<ui16>(DateUpperBound - 1, /*tzId*/ 1);
    builder.Add(
        "tz_date_as_string32",
        CreateSimpleTypeSchema(EWireType::String32),
        TzDate(),
        tzDate,
        [&] (TCheckedSkiffWriter* writer) {
            writer->WriteString32(tzDate);
        });

    // Inside a struct the tz value goes through the YSON converter instead of the column path.
    auto tzTimestamp64 = MakeTzString<i64>(/*timeValue*/ 42, /*tzId*/ 1);
    builder.Add(
        "tz_timestamp64_in_struct",
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("key"),
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Int64),
                CreateSimpleTypeSchema(EWireType::Uint16),
            })->SetName("value"),
        }),
        Struct("key", String(), "value", TzTimestamp64()),
        TNamedValue::TComposite{
            BuildYsonStringFluently()
                .BeginList()
                    .Item().Value("row_0")
                    .Item().Value(tzTimestamp64)
                .EndList()
                .ToString(),
        },
        [] (TCheckedSkiffWriter* writer) {
            writer->WriteString32("row_0");
            writer->WriteInt64(42);
            writer->WriteUint16(/*tzId*/ 1);
        });

    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    TzTypes,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeTzTypeCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeUuidCases()
{
    auto uuid = "\xee\x1f\x37\x70" "\xb9\x93\x64\xb5" "\xe4\xdf\xe9\x03" "\x67\x5c\x30\x62"sv;
    // The 16 bytes read as a big-endian number.
    auto uuidAsUint128 = TUint128{.Low = 0xe4dfe903675c3062, .High = 0xee1f3770b99364b5};

    TValueCaseBuilder builder;
    std::vector<TLogicalTypePtr> logicalTypes = {Uuid(), Optional(Uuid())};
    for (const auto& logicalType : logicalTypes) {
        auto typeName = MakeTypeCaseName(*logicalType);
        builder.Add(
            typeName + "_as_uint128",
            CreateSimpleTypeSchema(EWireType::Uint128),
            logicalType,
            std::string(uuid),
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteUint128(uuidAsUint128);
            });
        builder.Add(
            typeName + "_as_string32",
            CreateSimpleTypeSchema(EWireType::String32),
            logicalType,
            std::string(uuid),
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteString32(uuid);
            });
        builder.Add(
            typeName + "_as_optional_uint128",
            CreateOptionalSchema(EWireType::Uint128),
            logicalType,
            std::string(uuid),
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant8Tag(1);
                writer->WriteUint128(uuidAsUint128);
            });
        builder.Add(
            typeName + "_as_optional_string32",
            CreateOptionalSchema(EWireType::String32),
            logicalType,
            std::string(uuid),
            [&] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant8Tag(1);
                writer->WriteString32(uuid);
            });
    }

    return builder.Cases;
}

INSTANTIATE_TEST_SUITE_P(
    Uuid,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeUuidCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeYsonCases()
{
    std::vector<TRoundTripCase> result;
    auto skiffSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::Yson32)->SetName("column")});

    const auto& examples = GetPrimitiveValueExamples();
    THashMap<std::string, int> typeNameToExampleCount;
    for (const auto& example : examples) {
        ++typeNameToExampleCount[MakeTypeCaseName(*example.LogicalType)];
    }

    THashMap<std::string, int> typeNameToExampleIndex;
    for (const auto& example : examples) {
        auto name = MakeTypeCaseName(*example.LogicalType);
        if (GetOrCrash(typeNameToExampleCount, name) > 1) {
            auto index = typeNameToExampleIndex[name]++;
            name += "_example_" + ToString(index);
        }
        result.push_back({
            .CaseName = name + "_schemaful",
            .SkiffSchema = skiffSchema,
            .TableSchema = MakeTableSchema({{"column", example.LogicalType}}),
            .Rows = {{{"column", example.Value}}},
            .Expected = TExpectedYson{example.PrettyYson},
        });
        result.push_back({
            .CaseName = name + "_schemaless",
            .SkiffSchema = skiffSchema,
            .Rows = {{{"column", example.Value}}},
            .Expected = TExpectedYson{example.PrettyYson},
        });
    }
    for (auto type : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
        auto logicalType = Optional(SimpleLogicalType(type));
        if (IsV3Composite(logicalType)) {
            continue;
        }
        result.push_back({
            .CaseName = MakeTypeCaseName(*logicalType) + "_null",
            .SkiffSchema = skiffSchema,
            .TableSchema = MakeTableSchema({{"column", logicalType}}),
            .Rows = {{{"column", nullptr}}},
            .Expected = TExpectedYson{"#"},
        });
    }

    auto ysonSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson32"),
        CreateOptionalSchema(EWireType::Yson32)->SetName("opt_yson32"),
    });
    result.push_back({
        .CaseName = "scalars_and_any_values",
        .SkiffSchema = ysonSchema,
        .Rows = {
            {{"yson32", nullptr}, {"opt_yson32", nullptr}},
            {{"yson32", -5}, {"opt_yson32", -6}},
            {{"yson32", 42u}, {"opt_yson32", 43u}},
            {{"yson32", 2.7182818}, {"opt_yson32", 3.1415926}},
            {{"yson32", true}, {"opt_yson32", false}},
            {{"yson32", "Yin"}, {"opt_yson32", "Yang"}},
            {
                {"yson32", EValueType::Any, ToBinaryYson("{foo=bar}")},
                {"opt_yson32", EValueType::Any, ToBinaryYson("{bar=baz}")},
            },
        },
        .Expected = MakeExpectedData(MakeSkiffData(ysonSchema, [] (TCheckedSkiffWriter* writer) {
            auto writeRow = [&] (TStringBuf yson, std::optional<TStringBuf> optionalYson) {
                writer->WriteVariant16Tag(0);
                writer->WriteYson32(yson);
                if (optionalYson) {
                    writer->WriteVariant8Tag(1);
                    writer->WriteYson32(*optionalYson);
                } else {
                    writer->WriteVariant8Tag(0);
                }
            };
            writeRow(ToBinaryYson("#"), std::nullopt);
            writeRow(ToBinaryYson("-5"), ToBinaryYson("-6"));
            writeRow(ToBinaryYson("42u"), ToBinaryYson("43u"));
            writeRow(ToBinaryYson("2.7182818"), ToBinaryYson("3.1415926"));
            writeRow(ToBinaryYson("%true"), ToBinaryYson("%false"));
            writeRow(ToBinaryYson("Yin"), ToBinaryYson("Yang"));
            writeRow(ToBinaryYson("{foo=bar}"), ToBinaryYson("{bar=baz}"));
        })),
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Yson,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeYsonCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeCompositeCases()
{
    std::vector<TRoundTripCase> result;

    auto pointsSchema = CreateTupleSchema({
        CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
            CreateRepeatedVariant8Schema({
                CreateTupleSchema({
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("x"),
                    CreateSimpleTypeSchema(EWireType::Int64)->SetName("y"),
                }),
            })->SetName("points"),
        })->SetName("value"),
    });
    result.push_back({
        .CaseName = "struct_with_list",
        .SkiffSchema = pointsSchema,
        .TableSchema = MakeTableSchema({
            {"value", Struct("name", String(), "points", List(Struct("x", Int64(), "y", Int64())))},
        }),
        .Rows = {{{"value", EValueType::Composite, "[foo;[[0;1];[2;3]]]"}}},
        .Expected = MakeExpectedData(MakeSkiffData(pointsSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteString32("foo");
            writer->WriteVariant8Tag(0);
            writer->WriteInt64(0);
            writer->WriteInt64(1);
            writer->WriteVariant8Tag(0);
            writer->WriteInt64(2);
            writer->WriteInt64(3);
            writer->WriteVariant8Tag(EndOfSequenceTag<ui8>());
        })),
    });

    auto nameValueSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    })->SetName("value");

    auto optionalStructType = Optional(Struct("name", String(), "value", String()));
    auto optionalSchema = CreateTupleSchema({
        CreateOptionalSchema(nameValueSchema)->SetName("value"),
    });
    result.push_back({
        .CaseName = "optional_struct_null",
        .SkiffSchema = optionalSchema,
        .TableSchema = MakeTableSchema({{"value", optionalStructType}}),
        .Rows = {{{"value", nullptr}}},
        .Expected = MakeExpectedData(MakeSkiffData(optionalSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(0);
        })),
    });

    auto sparseSchema = CreateTupleSchema({
        CreateSparseColumnsSchema({
            nameValueSchema,
        }),
    });
    result.push_back({
        .CaseName = "sparse_struct",
        .SkiffSchema = sparseSchema,
        .TableSchema = MakeTableSchema({{"value", optionalStructType}}),
        .Rows = {{{"value", EValueType::Composite, "[foo;bar]"}}, {}},
        .Expected = MakeExpectedData(MakeSkiffData(sparseSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
            writer->WriteString32("foo");
            writer->WriteString32("bar");
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
        })),
    });

    auto sparseOptionalSchema = CreateTupleSchema({
        CreateSparseColumnsSchema({
            CreateOptionalSchema(nameValueSchema)->SetName("value"),
        }),
    });
    result.push_back({
        .CaseName = "sparse_optional_struct",
        .SkiffSchema = sparseOptionalSchema,
        .TableSchema = MakeTableSchema({{"value", optionalStructType}}),
        .Rows = {{{"value", EValueType::Composite, "[foo;bar]"}}, {}},
        .Expected = MakeExpectedData(MakeSkiffData(sparseOptionalSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteString32("foo");
            writer->WriteString32("bar");
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
        })),
    });

    auto optionalNothingSchema = CreateTupleSchema({CreateOptionalSchema(EWireType::Nothing)->SetName("opt_null")});
    for (auto singularType : {ESimpleLogicalValueType::Null, ESimpleLogicalValueType::Void}) {
        result.push_back({
            .CaseName = Format("optional_of_%lv", singularType),
            .SkiffSchema = optionalNothingSchema,
            .TableSchema = MakeTableSchema({{"opt_null", Optional(SimpleLogicalType(singularType))}}),
            .Rows = {{{"opt_null", nullptr}}, {{"opt_null", EValueType::Composite, "[#]"}}},
            .Expected = MakeExpectedData(MakeSkiffData(optionalNothingSchema, [] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant16Tag(0);
                writer->WriteVariant8Tag(0);
                writer->WriteVariant16Tag(0);
                writer->WriteVariant8Tag(1);
            })),
        });
    }

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Composites,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeCompositeCases()),
    GetCaseName<TRoundTripCase>);

std::vector<TRoundTripCase> MakeColumnCases()
{
    std::vector<TRoundTripCase> result;

    auto valuesOutOfOrderSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateOptionalSchema(EWireType::String32)->SetName("eng"),
        CreateOptionalSchema(EWireType::String32)->SetName("rus"),
    });
    result.push_back({
        .CaseName = "values_out_of_order",
        .SkiffSchema = valuesOutOfOrderSchema,
        .Rows = {
            {{"number", 1}, {"eng", "one"}, {"rus", nullptr}},
            {{"eng", nullptr}, {"number", 2}, {"rus", "dva"}},
            {{"rus", "tri"}, {"eng", "three"}, {"number", 3}},
        },
        .Expected = MakeExpectedData(MakeSkiffData(valuesOutOfOrderSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(1);
            writer->WriteVariant8Tag(1);
            writer->WriteString32("one");
            writer->WriteVariant8Tag(0);

            writer->WriteVariant16Tag(0);
            writer->WriteInt64(2);
            writer->WriteVariant8Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteString32("dva");

            writer->WriteVariant16Tag(0);
            writer->WriteInt64(3);
            writer->WriteVariant8Tag(1);
            writer->WriteString32("three");
            writer->WriteVariant8Tag(1);
            writer->WriteString32("tri");
        })),
    });

    auto sparseSchema = CreateTupleSchema({
        CreateSparseColumnsSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("string32"),
        }),
    });
    result.push_back({
        .CaseName = "sparse_columns",
        .SkiffSchema = sparseSchema,
        .Rows = {
            {{"int64", -1}, {"string32", "minus one"}},
            {{"string32", "minus five"}, {"int64", -5}},
            {{"uint64", 42u}},
            {{"int64", -8}},
            {},
        },
        .Expected = MakeExpectedData(MakeSkiffData(sparseSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(-1);
            writer->WriteVariant16Tag(2);
            writer->WriteString32("minus one");
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());

            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(2);
            writer->WriteString32("minus five");
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(-5);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());

            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(1);
            writer->WriteUint64(42);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());

            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(-8);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());

            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
        })),
    });

    // The parser reports a missing dense optional column as an explicit null, hence the nullptr
    // values below.
    auto otherColumnsSchema = CreateTupleSchema({
        CreateOptionalSchema(EWireType::Int64)->SetName("int64_column"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName(TString(OtherColumnsName)),
    });
    result.push_back({
        .CaseName = "other_columns",
        .SkiffSchema = otherColumnsSchema,
        .Rows = {
            {{"int64_column", nullptr}, {"string_column", "foo"}},
            {{"int64_column", 42}},
            {{"int64_column", nullptr}, {"other_string_column", "bar"}},
        },
        .Expected = MakeExpectedData(MakeSkiffData(otherColumnsSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(0);
            writer->WriteYson32(ToBinaryYson("{string_column=foo}"));

            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(42);
            writer->WriteYson32(ToBinaryYson("{}"));

            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(0);
            writer->WriteYson32(ToBinaryYson("{other_string_column=bar}"));
        })),
    });

    auto reorderedNameTable = New<TNameTable>();
    reorderedNameTable->RegisterName("field_b");
    auto twoFieldSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("field_a"),
        CreateSimpleTypeSchema(EWireType::Uint64)->SetName("field_b"),
    });
    result.push_back({
        .CaseName = "column_ids_out_of_order",
        .SkiffSchema = twoFieldSchema,
        .Rows = {{{"field_a", -1}, {"field_b", 2u}}},
        .Expected = MakeExpectedData(MakeSkiffData(twoFieldSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(-1);
            writer->WriteUint64(2);
        })),
        .NameTable = reorderedNameTable,
    });

    auto emptySchema = CreateTupleSchema({});
    result.push_back({
        .CaseName = "no_columns",
        .SkiffSchema = emptySchema,
        .Rows = {{}, {}},
        .Expected = MakeExpectedData(MakeSkiffData(emptySchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
        })),
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Columns,
    TSkiffRoundTripTest,
    ::testing::ValuesIn(MakeColumnCases()),
    GetCaseName<TRoundTripCase>);

////////////////////////////////////////////////////////////////////////////////
// Writer only: features that exist only on output, and writer errors.

struct TWriterCase
{
    std::string CaseName;
    TSkiffSchemaPtr SkiffSchema;
    TTableSchemaPtr TableSchema = New<TTableSchema>();
    TNamedRows Rows;
    std::variant<TExpectedData, TExpectedError> Expected;
    TSkiffWriterOptions WriterOptions;
    TNameTablePtr NameTable;
};

class TSkiffWriterTest
    : public ::testing::TestWithParam<TWriterCase>
{ };

TEST_P(TSkiffWriterTest, Write)
{
    const auto& testCase = GetParam();
    auto write = [&] {
        return WriteSkiff(
            testCase.SkiffSchema,
            testCase.Rows,
            testCase.TableSchema,
            testCase.NameTable,
            testCase.WriterOptions);
    };
    if (const auto* data = std::get_if<TExpectedData>(&testCase.Expected)) {
        EXPECT_EQ(HexEncode(write()), data->Hex);
    } else {
        EXPECT_THROW_WITH_SUBSTRING(write(), std::get<TExpectedError>(testCase.Expected).Substring);
    }
}

std::vector<TWriterCase> MakeSmallIntWriterCases()
{
    std::vector<TWriterCase> result;
    auto addOutOfRange = [&] (EWireType wireType, const TLogicalTypePtr& logicalType, auto value) {
        auto prefix = std::string(ToString(wireType)) + "_" + MakeValueCaseName(value);
        result.push_back({
            .CaseName = prefix + "_out_of_range",
            .SkiffSchema = CreateTupleSchema({CreateSimpleTypeSchema(wireType)->SetName("column")}),
            .TableSchema = MakeTableSchema({{"column", logicalType}}),
            .Rows = {{{"column", value}}},
            .Expected = TExpectedError{Format(
                "Value %v is out of range for possible values for skiff type %Qlv",
                value,
                wireType)},
        });
    };
    for (auto [wireType, minValue, maxValue] : SignedSmallIntLimits) {
        addOutOfRange(wireType, Int64(), maxValue + 1);
        addOutOfRange(wireType, Int64(), minValue - 1);
    }
    for (auto [wireType, maxValue] : UnsignedSmallIntLimits) {
        addOutOfRange(wireType, Uint64(), maxValue + 1);
    }

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    SmallInts,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeSmallIntWriterCases()),
    GetCaseName<TWriterCase>);

std::vector<TWriterCase> MakeUuidWriterCases()
{
    std::vector<TWriterCase> result;
    result.push_back({
        .CaseName = "null_uuid_into_required_uint128",
        .SkiffSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::Uint128)->SetName("uuid")}),
        .TableSchema = MakeTableSchema({{"uuid", Optional(Uuid())}}),
        .Rows = {{{"uuid", nullptr}}},
        .Expected = TExpectedError{
            "Unexpected type of \"uuid\" column: "
            "Skiff format expected \"string\", actual table type \"null\""},
    });
    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Uuid,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeUuidWriterCases()),
    GetCaseName<TWriterCase>);

std::vector<TWriterCase> MakeCompositeWriterCases()
{
    std::vector<TWriterCase> result;

    auto listSchema = CreateRepeatedVariant8Schema({CreateSimpleTypeSchema(EWireType::Int64)})->SetName("list");
    result.push_back({
        .CaseName = "required_complex_field_for_missing_column",
        .SkiffSchema = CreateTupleSchema({listSchema}),
        .Expected = TExpectedError{
            "Unexpected wire type: expected one of \"int8\", \"int16\", \"int32\", \"int64\", "
            "\"uint8\", \"uint16\", \"uint32\", \"uint64\", \"string32\", \"boolean\", \"double\", "
            "\"nothing\", \"yson32\", got \"repeated_variant8\""},
    });

    auto optionalListSchema = CreateTupleSchema({
        CreateOptionalSchema(listSchema)->SetName("opt_list"),
    });
    result.push_back({
        .CaseName = "optional_complex_field_for_missing_column",
        .SkiffSchema = optionalListSchema,
        .Rows = {{}, {{"opt_list", nullptr}}, {}},
        .Expected = MakeExpectedData(MakeSkiffData(optionalListSchema, [] (TCheckedSkiffWriter* writer) {
            for (int rowIndex = 0; rowIndex < 3; ++rowIndex) {
                writer->WriteVariant16Tag(0);
                writer->WriteVariant8Tag(0);
            }
        })),
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Composites,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeCompositeWriterCases()),
    GetCaseName<TWriterCase>);

std::vector<TWriterCase> MakeColumnWriterCases()
{
    std::vector<TWriterCase> result;

    auto ysonSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson32"),
        CreateOptionalSchema(EWireType::Yson32)->SetName("opt_yson32"),
    });
    result.push_back({
        .CaseName = "missing_yson_values",
        .SkiffSchema = ysonSchema,
        .Rows = {{}},
        .Expected = MakeExpectedData(MakeSkiffData(ysonSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteYson32(ToBinaryYson("#"));
            writer->WriteVariant8Tag(0);
        })),
    });

    auto sparseSchema = CreateTupleSchema({
        CreateSparseColumnsSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("int64"),
            CreateSimpleTypeSchema(EWireType::Uint64)->SetName("uint64"),
        }),
    });
    result.push_back({
        .CaseName = "sparse_explicit_nulls_are_skipped",
        .SkiffSchema = sparseSchema,
        .Rows = {{{"int64", -8}, {"uint64", nullptr}}},
        .Expected = MakeExpectedData(MakeSkiffData(sparseSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(-8);
            writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
        })),
    });

    result.push_back({
        .CaseName = "missing_required_field",
        .SkiffSchema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("eng"),
        }),
        .Rows = {{{"number", 1}}},
        .Expected = TExpectedError{
            "Unexpected type of \"eng\" column: "
            "Skiff format expected \"string\", actual table type \"null\""},
    });

    auto valueSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::String32)->SetName("value")});
    result.push_back({
        .CaseName = "unknown_column",
        .SkiffSchema = valueSchema,
        .Rows = {{{"unknown_column", "four"}}},
        .Expected = TExpectedError{"Column \"unknown_column\" is not described by Skiff schema"},
    });

    auto nameTable = New<TNameTable>();
    nameTable->RegisterName("unknown_column");
    result.push_back({
        .CaseName = "unknown_column_registered_before_writer",
        .SkiffSchema = valueSchema,
        .Rows = {{{"unknown_column", "four"}}},
        .Expected = TExpectedError{"Column \"unknown_column\" is not described by Skiff schema"},
        .NameTable = nameTable,
    });

    result.push_back({
        .CaseName = "variant8_of_optional",
        .SkiffSchema = CreateTupleSchema({
            CreateVariant8Schema({CreateOptionalSchema(EWireType::Yson32)})->SetName("opt_yson32"),
        }),
        .Expected = TExpectedError{
            "Unexpected wire type: expected one of \"int8\", \"int16\", \"int32\", \"int64\", "
            "\"uint8\", \"uint16\", \"uint32\", \"uint64\", \"string32\", \"boolean\", \"double\", "
            "\"nothing\", \"yson32\", got \"variant8\""},
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Columns,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeColumnWriterCases()),
    GetCaseName<TWriterCase>);

// Tags: 0 = the index follows from the previous row (TOmittedIndex), 1 = explicit index,
// 2 = the reader did not return the index (TMissingIndex); without this alternative, the
// writer throws.
TSkiffSchemaPtr CreateIndexColumnSchema(const std::string& name, bool allowMissing = false)
{
    TSkiffSchemaList alternatives = {
        CreateSimpleTypeSchema(EWireType::Nothing),
        CreateSimpleTypeSchema(EWireType::Int64),
    };
    if (allowMissing) {
        alternatives.push_back(CreateSimpleTypeSchema(EWireType::Nothing));
    }
    return CreateVariant8Schema(std::move(alternatives))->SetName(TString(name));
}

struct TOmittedIndex
{ };

struct TMissingIndex
{ };

using TIndexOutput = std::variant<i64, TOmittedIndex, TMissingIndex>;

constexpr TIndexOutput OmittedIndex{TOmittedIndex{}};
constexpr TIndexOutput MissingIndex{TMissingIndex{}};

struct TInputIndices
{
    std::optional<int> RangeIndex;
    std::optional<int> RowIndex;
};

struct TExpectedIndices
{
    TIndexOutput RangeIndex;
    TIndexOutput RowIndex;
};

std::vector<TWriterCase> MakeRowRangeIndexWriterCases()
{
    auto strictSchema = CreateTupleSchema({
        CreateIndexColumnSchema(RangeIndexColumnName),
        CreateIndexColumnSchema(RowIndexColumnName),
    });
    auto allowMissingSchema = CreateTupleSchema({
        CreateIndexColumnSchema(RangeIndexColumnName, /*allowMissing*/ true),
        CreateIndexColumnSchema(RowIndexColumnName, /*allowMissing*/ true),
    });
    auto makeRows = [] (const std::vector<TInputIndices>& indices) {
        TNamedRows result;
        for (const auto& [rangeIndex, rowIndex] : indices) {
            TNamedRow row;
            if (rangeIndex) {
                row.emplace_back(RangeIndexColumnName, *rangeIndex);
            }
            if (rowIndex) {
                row.emplace_back(RowIndexColumnName, *rowIndex);
            }
            result.push_back(std::move(row));
        }
        return result;
    };
    auto makeExpected = [] (
        const TSkiffSchemaPtr& schema,
        const std::vector<TExpectedIndices>& indices)
    {
        return MakeExpectedData(MakeSkiffData(schema, [&] (TCheckedSkiffWriter* writer) {
            auto writeIndex = [&] (const TIndexOutput& index) {
                if (const auto* value = std::get_if<i64>(&index)) {
                    writer->WriteVariant8Tag(1);
                    writer->WriteInt64(*value);
                } else if (std::holds_alternative<TMissingIndex>(index)) {
                    writer->WriteVariant8Tag(2);
                } else {
                    writer->WriteVariant8Tag(0);
                }
            };
            for (const auto& [rangeIndex, rowIndex] : indices) {
                writer->WriteVariant16Tag(0);
                writeIndex(rangeIndex);
                writeIndex(rowIndex);
            }
        }));
    };

    std::vector<TWriterCase> result;

    for (bool allowMissing : {false, true}) {
        const auto& schema = allowMissing ? allowMissingSchema : strictSchema;
        std::string namePrefix = allowMissing ? "allow_missing_" : "";
        result.push_back({
            .CaseName = namePrefix + "consecutive",
            .SkiffSchema = schema,
            .Rows = makeRows({{0, 0}, {0, 1}, {0, 2}}),
            .Expected = makeExpected(schema, {{0, 0}, {OmittedIndex, OmittedIndex}, {OmittedIndex, OmittedIndex}}),
        });
        result.push_back({
            .CaseName = namePrefix + "row_gap",
            .SkiffSchema = schema,
            .Rows = makeRows({{0, 0}, {0, 1}, {0, 3}}),
            .Expected = makeExpected(schema, {{0, 0}, {OmittedIndex, OmittedIndex}, {OmittedIndex, 3}}),
        });
        result.push_back({
            .CaseName = namePrefix + "range_change",
            .SkiffSchema = schema,
            .Rows = makeRows({{0, 0}, {0, 1}, {1, 2}, {1, 3}}),
            .Expected = makeExpected(schema, {
                {0, 0},
                {OmittedIndex, OmittedIndex},
                {1, 2},
                {OmittedIndex, OmittedIndex},
            }),
        });
    }

    result.push_back({
        .CaseName = "row_index_missing",
        .SkiffSchema = strictSchema,
        .Rows = {{{RangeIndexColumnName, 0}}},
        .Expected = TExpectedError{"Row index requested but reader did not return it"},
    });
    result.push_back({
        .CaseName = "range_index_missing",
        .SkiffSchema = strictSchema,
        .Rows = {{{RowIndexColumnName, 0}}},
        .Expected = TExpectedError{"Range index requested but reader did not return it"},
    });
    result.push_back({
        .CaseName = "allow_missing_both_indices_missing",
        .SkiffSchema = allowMissingSchema,
        .Rows = makeRows({{{}, {}}, {{}, {}}}),
        .Expected = makeExpected(allowMissingSchema, {{MissingIndex, MissingIndex}, {MissingIndex, MissingIndex}}),
    });
    result.push_back({
        .CaseName = "allow_missing_range_index_missing",
        .SkiffSchema = allowMissingSchema,
        .Rows = makeRows({{{}, 0}, {{}, 1}, {{}, 3}, {{}, 4}}),
        .Expected = makeExpected(allowMissingSchema, {
            {MissingIndex, 0},
            {MissingIndex, OmittedIndex},
            {MissingIndex, 3},
            {MissingIndex, OmittedIndex},
        }),
    });
    result.push_back({
        .CaseName = "allow_missing_row_index_missing",
        .SkiffSchema = allowMissingSchema,
        .Rows = makeRows({{0, {}}, {0, {}}, {1, {}}, {1, {}}}),
        .Expected = makeExpected(allowMissingSchema, {
            {0, MissingIndex},
            {OmittedIndex, MissingIndex},
            {1, MissingIndex},
            {OmittedIndex, MissingIndex},
        }),
    });

    for (const auto& columnName : {RowIndexColumnName, RangeIndexColumnName}) {
        auto schema = CreateTupleSchema({CreateIndexColumnSchema(columnName)});
        result.push_back({
            .CaseName = "only_" + columnName.substr(1),
            .SkiffSchema = schema,
            .Rows = {{{columnName, 0}}},
            .Expected = MakeExpectedData(MakeSkiffData(schema, [] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant16Tag(0);
                writer->WriteVariant8Tag(1);
                writer->WriteInt64(0);
            })),
        });
    }

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    RowRangeIndex,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeRowRangeIndexWriterCases()),
    GetCaseName<TWriterCase>);

std::vector<TWriterCase> MakeControlAttributeWriterCases()
{
    std::vector<TWriterCase> result;

    auto keySwitchSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName(TString(KeySwitchColumnName)),
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("value1"),
    });
    result.push_back({
        .CaseName = "key_switch",
        .SkiffSchema = keySwitchSchema,
        .Rows = {
            {{"value", "one"}, {"value1", 0}},
            {{"value", "one"}, {"value1", 1}},
            {{"value", "two"}, {"value1", 2}},
        },
        .Expected = MakeExpectedData(MakeSkiffData(keySwitchSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteString32("one");
            writer->WriteBoolean(false);
            writer->WriteInt64(0);

            writer->WriteVariant16Tag(0);
            writer->WriteString32("one");
            writer->WriteBoolean(false);
            writer->WriteInt64(1);

            writer->WriteVariant16Tag(0);
            writer->WriteString32("two");
            writer->WriteBoolean(true);
            writer->WriteInt64(2);
        })),
        .WriterOptions = {.KeyColumnCount = 1, .EnableKeySwitch = true},
    });

    auto valueSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::String32)->SetName("value")});
    result.push_back({
        .CaseName = "end_of_stream",
        .SkiffSchema = valueSchema,
        .Rows = {{{"value", "zero"}}, {{"value", "one"}}},
        .Expected = MakeExpectedData(MakeSkiffData(
            valueSchema,
            [] (TCheckedSkiffWriter* writer) {
                writer->WriteVariant16Tag(0);
                writer->WriteString32("zero");
                writer->WriteVariant16Tag(0);
                writer->WriteString32("one");
                writer->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            },
            /*endOfStream*/ true)),
        .WriterOptions = {.EnableEndOfStream = true},
    });

    result.push_back({
        .CaseName = "zero_table_index_is_accepted",
        .SkiffSchema = valueSchema,
        .Rows = {{{"value", "zero"}, {TableIndexColumnName, 0}}},
        .Expected = MakeExpectedData(MakeSkiffData(valueSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteString32("zero");
        })),
    });

    auto remainingRowBytesSchema = CreateTupleSchema({
        CreateIndexColumnSchema(RowIndexColumnName),
        CreateSimpleTypeSchema(EWireType::Int32)->SetName(TString(RemainingRowBytesColumnName)),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("data"),
    });
    result.push_back({
        .CaseName = "remaining_row_bytes",
        .SkiffSchema = remainingRowBytesSchema,
        .TableSchema = MakeTableSchema({{"data", Optional(String())}}),
        .Rows = {
            {{RowIndexColumnName, 0}, {"data", "abcdef"}},
            {{RowIndexColumnName, 2}, {"data", "xyz"}},
        },
        .Expected = MakeExpectedData(MakeSkiffData(remainingRowBytesSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(0);
            // The remaining bytes are the string32 that follows: its length prefix and payload.
            writer->WriteInt32(4 + 6);
            writer->WriteString32("abcdef");

            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(2);
            writer->WriteInt32(4 + 3);
            writer->WriteString32("xyz");
        })),
    });

    result.push_back({
        .CaseName = "duplicate_special_column",
        .SkiffSchema = CreateTupleSchema({
            CreateSimpleTypeSchema(EWireType::Int32)->SetName(TString(RemainingRowBytesColumnName)),
            CreateIndexColumnSchema(RowIndexColumnName),
            CreateSimpleTypeSchema(EWireType::Int32)->SetName(TString(RemainingRowBytesColumnName)),
            CreateSimpleTypeSchema(EWireType::String32)->SetName("data"),
        }),
        .Expected = TExpectedError{"Name \"$remaining_row_bytes\" is found multiple times"},
    });

    auto skippedFieldsSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::Int64)->SetName("number"),
        CreateSimpleTypeSchema(EWireType::Nothing)->SetName("string"),
        CreateIndexColumnSchema(RangeIndexColumnName),
        CreateIndexColumnSchema(RowIndexColumnName),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName(TString(KeySwitchColumnName)),
        CreateSimpleTypeSchema(EWireType::Double)->SetName("double"),
    });
    result.push_back({
        .CaseName = "skipped_fields",
        .SkiffSchema = skippedFieldsSchema,
        .TableSchema = MakeTableSchema({
            {"number", Optional(Int64())},
            {"string", Optional(String())},
            {"double", Optional(Double())},
        }),
        .Rows = {
            {
                {"number", 1},
                {"string", "hello"},
                {RangeIndexColumnName, 0},
                {RowIndexColumnName, 0},
                {"double", 1.5},
            },
            {{"number", 2}, {RangeIndexColumnName, 5}, {RowIndexColumnName, 1}, {"double", 2.5}},
        },
        .Expected = MakeExpectedData(MakeSkiffData(skippedFieldsSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteInt64(1);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(0);
            writer->WriteBoolean(false);
            writer->WriteDouble(1.5);

            writer->WriteVariant16Tag(0);
            writer->WriteInt64(2);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(5);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(1);
            writer->WriteBoolean(true);
            writer->WriteDouble(2.5);
        })),
        .WriterOptions = {.KeyColumnCount = 1, .EnableKeySwitch = true},
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    ControlAttributes,
    TSkiffWriterTest,
    ::testing::ValuesIn(MakeControlAttributeWriterCases()),
    GetCaseName<TWriterCase>);

////////////////////////////////////////////////////////////////////////////////
// Parser only: inputs the writer would not produce, and parser errors.

struct TExpectedRows
{
    TNamedRows Rows;
};

struct TParserCase
{
    std::string CaseName;
    TSkiffSchemaPtr SkiffSchema;
    TTableSchemaPtr TableSchema = New<TTableSchema>();
    std::string Data;
    std::variant<TExpectedRows, TExpectedError> Expected;
};

class TSkiffParserTest
    : public ::testing::TestWithParam<TParserCase>
{ };

TEST_P(TSkiffParserTest, Parse)
{
    const auto& testCase = GetParam();
    auto parse = [&] {
        return ParseSkiff(testCase.SkiffSchema, testCase.Data, testCase.TableSchema);
    };
    if (const auto* rows = std::get_if<TExpectedRows>(&testCase.Expected)) {
        EXPECT_EQ(parse(), CanonizeRows(rows->Rows));
    } else {
        EXPECT_THROW_WITH_SUBSTRING(parse(), std::get<TExpectedError>(testCase.Expected).Substring);
    }
}

std::vector<TParserCase> MakeYsonParserCases()
{
    std::vector<TParserCase> result;

    // External Skiff writers may send text YSON.
    auto ysonSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::Yson32)->SetName("yson")});
    result.push_back({
        .CaseName = "text_yson_values",
        .SkiffSchema = ysonSchema,
        .Data = MakeSkiffData(ysonSchema, [] (TCheckedSkiffWriter* writer) {
            for (auto yson : {"-42", "42u", "\"foobar\"", "%true", "{foo=bar}", "#"}) {
                writer->WriteVariant16Tag(0);
                writer->WriteYson32(yson);
            }
        }),
        .Expected = TExpectedRows{.Rows = {
            {{"yson", -42}},
            {{"yson", 42u}},
            {{"yson", "foobar"}},
            {{"yson", true}},
            {{"yson", EValueType::Any, "{foo=bar}"}},
            {{"yson", nullptr}},
        }},
    });

    auto otherColumnsSchema = CreateTupleSchema({
        CreateSimpleTypeSchema(EWireType::String32)->SetName("name"),
        CreateSimpleTypeSchema(EWireType::Yson32)->SetName(TString(OtherColumnsName)),
    });
    result.push_back({
        .CaseName = "text_yson_other_columns",
        .SkiffSchema = otherColumnsSchema,
        .Data = MakeSkiffData(otherColumnsSchema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteString32("row_0");
            writer->WriteYson32("{foo=-42;}");
            writer->WriteVariant16Tag(0);
            writer->WriteString32("row_1");
            writer->WriteYson32("{bar=qux;baz={boolean=%false;};}");
        }),
        .Expected = TExpectedRows{.Rows = {
            {{"name", "row_0"}, {"foo", -42}},
            {{"name", "row_1"}, {"bar", "qux"}, {"baz", EValueType::Any, "{boolean=%false}"}},
        }},
    });

    auto makeYsonData = [&] (TStringBuf yson) {
        return MakeSkiffData(ysonSchema, [&] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteYson32(yson);
        });
    };
    result.push_back({
        .CaseName = "truncated_yson",
        .SkiffSchema = ysonSchema,
        .Data = makeYsonData("[42"),
        .Expected = TExpectedError{"Premature end of stream"},
    });
    result.push_back({
        .CaseName = "yson_with_attributes",
        .SkiffSchema = ysonSchema,
        .Data = makeYsonData("<foo=bar>42"),
        .Expected = TExpectedError{"Table values cannot have top-level attributes"},
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Yson,
    TSkiffParserTest,
    ::testing::ValuesIn(MakeYsonParserCases()),
    GetCaseName<TParserCase>);

std::vector<TParserCase> MakeTzTypeParserCases()
{
    std::vector<TParserCase> result;
    result.push_back({
        .CaseName = "tz_datetime_as_uint16",
        .SkiffSchema = CreateTupleSchema({
            CreateTupleSchema({
                CreateSimpleTypeSchema(EWireType::Uint16),
                CreateSimpleTypeSchema(EWireType::Uint16),
            })->SetName("date"),
        }),
        .TableSchema = MakeTableSchema({{"date", TzDatetime()}}),
        .Expected = TExpectedError{"TzType cannot be represented with Skiff schema"},
    });
    return result;
}

INSTANTIATE_TEST_SUITE_P(
    TzTypes,
    TSkiffParserTest,
    ::testing::ValuesIn(MakeTzTypeParserCases()),
    GetCaseName<TParserCase>);

std::vector<TParserCase> MakeSpecialColumnParserCases()
{
    std::vector<TParserCase> result;

    auto schema = CreateTupleSchema({
        CreateIndexColumnSchema(RangeIndexColumnName),
        CreateIndexColumnSchema(RowIndexColumnName),
        CreateSimpleTypeSchema(EWireType::Boolean)->SetName(TString(KeySwitchColumnName)),
        CreateSimpleTypeSchema(EWireType::String32)->SetName("value"),
    });
    result.push_back({
        .CaseName = "index_and_key_switch_are_ordinary_columns",
        .SkiffSchema = schema,
        .Data = MakeSkiffData(schema, [] (TCheckedSkiffWriter* writer) {
            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(0);
            writer->WriteVariant8Tag(1);
            writer->WriteInt64(7);
            writer->WriteBoolean(true);
            writer->WriteString32("one");

            writer->WriteVariant16Tag(0);
            writer->WriteVariant8Tag(0);
            writer->WriteVariant8Tag(0);
            writer->WriteBoolean(false);
            writer->WriteString32("two");
        }),
        .Expected = TExpectedRows{.Rows = {
            {
                {RangeIndexColumnName, 0},
                {RowIndexColumnName, 7},
                {KeySwitchColumnName, true},
                {"value", "one"},
            },
            {
                {RangeIndexColumnName, nullptr},
                {RowIndexColumnName, nullptr},
                {KeySwitchColumnName, false},
                {"value", "two"},
            },
        }},
    });

    result.push_back({
        .CaseName = "allow_missing_index_is_rejected",
        .SkiffSchema = CreateTupleSchema({CreateIndexColumnSchema(RowIndexColumnName, /*allowMissing*/ true)}),
        .Expected = TExpectedError{"Cannot create Skiff parser for column \"$row_index\""},
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    SpecialColumns,
    TSkiffParserTest,
    ::testing::ValuesIn(MakeSpecialColumnParserCases()),
    GetCaseName<TParserCase>);

std::vector<TParserCase> MakeColumnParserCases()
{
    std::vector<TParserCase> result;

    result.push_back({
        .CaseName = "optional_nothing_schemaless",
        .SkiffSchema = CreateTupleSchema({CreateOptionalSchema(EWireType::Nothing)->SetName("opt_null")}),
        .Expected = TExpectedError{"Column \"opt_null\" cannot be represented with Skiff schema"},
    });

    result.push_back({
        .CaseName = "variant8_of_optional",
        .SkiffSchema = CreateTupleSchema({
            CreateVariant8Schema({CreateOptionalSchema(EWireType::Yson32)})->SetName("opt_yson32"),
        }),
        .Expected = TExpectedError{
            "Unexpected wire type: expected one of \"int8\", \"int16\", \"int32\", \"int64\", "
            "\"uint8\", \"uint16\", \"uint32\", \"uint64\", \"double\", \"boolean\", \"string32\", "
            "\"nothing\", \"yson32\", got \"variant8\""},
    });

    return result;
}

INSTANTIATE_TEST_SUITE_P(
    Columns,
    TSkiffParserTest,
    ::testing::ValuesIn(MakeColumnParserCases()),
    GetCaseName<TParserCase>);

TEST(TSkiffParserEmptyInputTest, YieldsNoRows)
{
    auto skiffSchema = CreateTupleSchema({CreateSimpleTypeSchema(EWireType::String32)->SetName("column")});
    for (int emptyReadCount : {0, 1, 2}) {
        TCollectingValueConsumer rowCollector;
        auto parser = CreateParserForSkiff(skiffSchema, &rowCollector);
        for (int readIndex = 0; readIndex < emptyReadCount; ++readIndex) {
            parser->Read("");
        }
        parser->Finish();
        EXPECT_EQ(rowCollector.Size(), 0) << "emptyReadCount = " << emptyReadCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
