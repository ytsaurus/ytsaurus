#include "row_helpers.h"

#include <yt/ytlib/unittests/protobuf_format_ut.pb.h>

#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>
#include <yt/core/ytree/fluent.h>

#include <yt/client/formats/config.h>
#include <yt/client/formats/parser.h>
#include <yt/client/formats/lenval_control_constants.h>
#include <yt/client/formats/protobuf_writer.h>
#include <yt/client/formats/protobuf_parser.h>
#include <yt/client/formats/format.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/value_consumer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/async_stream.h>

#include <util/random/fast.h>


namespace NYT {
namespace {

using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// Hadcoded serialization of file descriptor used in old format description.
TString FileDescriptor = "\x0a\xb6\x03\x0a\x29\x6a\x75\x6e\x6b\x2f\x65\x72\x6d\x6f\x6c\x6f\x76\x64\x2f\x74\x65\x73\x74\x2d\x70\x72\x6f\x74\x6f\x62"
    "\x75\x66\x2f\x6d\x65\x73\x73\x61\x67\x65\x2e\x70\x72\x6f\x74\x6f\x22\x2d\x0a\x0f\x54\x45\x6d\x62\x65\x64\x65\x64\x4d\x65\x73\x73\x61\x67\x65\x12"
    "\x0b\x0a\x03\x4b\x65\x79\x18\x01\x20\x01\x28\x09\x12\x0d\x0a\x05\x56\x61\x6c\x75\x65\x18\x02\x20\x01\x28\x09\x22\xb3\x02\x0a\x08\x54\x4d\x65\x73"
    "\x73\x61\x67\x65\x12\x0e\x0a\x06\x44\x6f\x75\x62\x6c\x65\x18\x01\x20\x01\x28\x01\x12\x0d\x0a\x05\x46\x6c\x6f\x61\x74\x18\x02\x20\x01\x28\x02\x12"
    "\x0d\x0a\x05\x49\x6e\x74\x36\x34\x18\x03\x20\x01\x28\x03\x12\x0e\x0a\x06\x55\x49\x6e\x74\x36\x34\x18\x04\x20\x01\x28\x04\x12\x0e\x0a\x06\x53\x49"
    "\x6e\x74\x36\x34\x18\x05\x20\x01\x28\x12\x12\x0f\x0a\x07\x46\x69\x78\x65\x64\x36\x34\x18\x06\x20\x01\x28\x06\x12\x10\x0a\x08\x53\x46\x69\x78\x65"
    "\x64\x36\x34\x18\x07\x20\x01\x28\x10\x12\x0d\x0a\x05\x49\x6e\x74\x33\x32\x18\x08\x20\x01\x28\x05\x12\x0e\x0a\x06\x55\x49\x6e\x74\x33\x32\x18\x09"
    "\x20\x01\x28\x0d\x12\x0e\x0a\x06\x53\x49\x6e\x74\x33\x32\x18\x0a\x20\x01\x28\x11\x12\x0f\x0a\x07\x46\x69\x78\x65\x64\x33\x32\x18\x0b\x20\x01\x28"
    "\x07\x12\x10\x0a\x08\x53\x46\x69\x78\x65\x64\x33\x32\x18\x0c\x20\x01\x28\x0f\x12\x0c\x0a\x04\x42\x6f\x6f\x6c\x18\x0d\x20\x01\x28\x08\x12\x0e\x0a"
    "\x06\x53\x74\x72\x69\x6e\x67\x18\x0e\x20\x01\x28\x09\x12\x0d\x0a\x05\x42\x79\x74\x65\x73\x18\x0f\x20\x01\x28\x0c\x12\x14\x0a\x04\x45\x6e\x75\x6d"
    "\x18\x10\x20\x01\x28\x0e\x32\x06\x2e\x45\x45\x6e\x75\x6d\x12\x21\x0a\x07\x4d\x65\x73\x73\x61\x67\x65\x18\x11\x20\x01\x28\x0b\x32\x10\x2e\x54\x45"
    "\x6d\x62\x65\x64\x65\x64\x4d\x65\x73\x73\x61\x67\x65\x2a\x24\x0a\x05\x45\x45\x6e\x75\x6d\x12\x07\x0a\x03\x4f\x6e\x65\x10\x01\x12\x07\x0a\x03\x54"
    "\x77\x6f\x10\x02\x12\x09\x0a\x05\x54\x68\x72\x65\x65\x10\x03";

TString GenerateRandomLenvalString(TFastRng64& rng, ui32 size)
{
    TString result;
    result.append(reinterpret_cast<const char*>(&size), sizeof(size));

    size += sizeof(ui32);

    while (result.size() < size) {
        ui64 num = rng.GenRand();
        result.append(reinterpret_cast<const char*>(&num), sizeof(num));
    }
    if (result.size() > size) {
        result.resize(size);
    }
    return result;
}

INodePtr ParseYson(TStringBuf data)
{
    return ConvertToNode(NYson::TYsonString(data.ToString()));
}

TProtobufFormatConfigPtr ParseFormatConfigFromNode(const INodePtr& configNode)
{
    auto config = New<NFormats::TProtobufFormatConfig>();
    config->Load(configNode);
    return config;
};

TProtobufFormatConfigPtr ParseProtobufFormatConfigFromString(TStringBuf configStr)
{
    return ParseFormatConfigFromNode(ParseYson(configStr));
}

TUnversionedOwningRow MakeRow(const std::initializer_list<TUnversionedValue>& rows)
{
    TUnversionedOwningRowBuilder builder;
    for (const auto& r : rows) {
        builder.AddValue(r);
    }

    return builder.FinishRow();
}

TString LenvalBytes(const ::google::protobuf::Message& message)
{
    TStringStream out;
    ui32 messageSize = message.ByteSize();
    out.Write(&messageSize, sizeof(messageSize));
    if (!message.SerializeToStream(&out)) {
        THROW_ERROR_EXCEPTION("Can not serialize message");
    }
    return out.Str();
}

void EnsureTypesMatch(EValueType expected, EValueType actual)
{
    if (expected != actual) {
        THROW_ERROR_EXCEPTION("Expected: %v actual: %v", expected, actual);
    }
}

double GetDouble(const TUnversionedValue& row)
{
    EnsureTypesMatch(EValueType::Double, row.Type);
    return row.Data.Double;
}

INodePtr CreateAllFieldsFileDescriptorConfig()
{
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("file_descriptor_set")
            .Value(FileDescriptor)
            .Item("file_indices")
            .BeginList()
                .Item().Value(0)
            .EndList()
            .Item("message_indices")
            .BeginList()
                .Item().Value(1)
            .EndList()
        .EndAttributes()
        .Value("protobuf");
}

INodePtr CreateAllFieldsSchemaConfig()
{
    return BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("enumerations")
            .BeginMap()
                .Item("EEnum")
                .BeginMap()
                    .Item("One").Value(1)
                    .Item("Two").Value(2)
                    .Item("Three").Value(3)
                    .Item("MinusFortyTwo").Value(-42)
                .EndMap()
            .EndMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Double")
                            .Item("field_number").Value(1)
                            .Item("proto_type").Value("double")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Float")
                            .Item("field_number").Value(2)
                            .Item("proto_type").Value("float")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Int64")
                            .Item("field_number").Value(3)
                            .Item("proto_type").Value("int64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("UInt64")
                            .Item("field_number").Value(4)
                            .Item("proto_type").Value("uint64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("SInt64")
                            .Item("field_number").Value(5)
                            .Item("proto_type").Value("sint64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Fixed64")
                            .Item("field_number").Value(6)
                            .Item("proto_type").Value("fixed64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("SFixed64")
                            .Item("field_number").Value(7)
                            .Item("proto_type").Value("sfixed64")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Int32")
                            .Item("field_number").Value(8)
                            .Item("proto_type").Value("int32")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("UInt32")
                            .Item("field_number").Value(9)
                            .Item("proto_type").Value("uint32")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("SInt32")
                            .Item("field_number").Value(10)
                            .Item("proto_type").Value("sint32")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Fixed32")
                            .Item("field_number").Value(11)
                            .Item("proto_type").Value("fixed32")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("SFixed32")
                            .Item("field_number").Value(12)
                            .Item("proto_type").Value("sfixed32")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Bool")
                            .Item("field_number").Value(13)
                            .Item("proto_type").Value("bool")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("String")
                            .Item("field_number").Value(14)
                            .Item("proto_type").Value("string")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Bytes")
                            .Item("field_number").Value(15)
                            .Item("proto_type").Value("bytes")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Enum")
                            .Item("field_number").Value(16)
                            .Item("proto_type").Value("enum_string")
                            .Item("enumeration_name").Value("EEnum")
                        .EndMap()

                        .Item()
                        .BeginMap()
                            .Item("name").Value("Message")
                            .Item("field_number").Value(17)
                            .Item("proto_type").Value("message")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("protobuf");
}

////////////////////////////////////////////////////////////////////////////////

struct TLenvalEntry
{
    TString RowData;
    ui32 TableIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalParser
{
public:
    explicit TLenvalParser(IInputStream* input)
        : Input_(input)
    { }

    TNullable<TLenvalEntry> Next()
    {
        ui32 rowSize;
        size_t read = Input_->Load(&rowSize, sizeof(rowSize));
        if (read == 0) {
            return Null;
        } else if (read < sizeof(rowSize)) {
            THROW_ERROR_EXCEPTION("corrupted lenval: can't read row length");
        }
        if (rowSize == LenvalTableIndexMarker) {
            ui32 tableIndex;
            read = Input_->Load(&tableIndex, sizeof(tableIndex));
            if (read != sizeof(tableIndex)) {
                THROW_ERROR_EXCEPTION("corrupted lenval: can't read table index");
            }
            CurrentTableIndex_ = tableIndex;
            return Next();
        } else if (
            rowSize == LenvalKeySwitch ||
            rowSize == LenvalRangeIndexMarker ||
            rowSize == LenvalRowIndexMarker)
        {
            THROW_ERROR_EXCEPTION("marker is unsupported");
        } else {
            TLenvalEntry result;
            result.RowData.resize(rowSize);
            result.TableIndex = CurrentTableIndex_;
            Input_->Load(result.RowData.Detach(), rowSize);

            return result;
        }
    }
private:
    IInputStream* Input_;
    ui32 CurrentTableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TProtobufFormat, TestConfigParsing)
{
    // Empty config.
    EXPECT_ANY_THROW(ParseProtobufFormatConfigFromString("{}"));

    // Broken protobuf
    EXPECT_ANY_THROW(ParseProtobufFormatConfigFromString(R"({file_descriptor_set="xx", file_indices=[0;], message_indices=[0;]})"));

    EXPECT_NO_THROW(ParseFormatConfigFromNode(
        CreateAllFieldsFileDescriptorConfig()->Attributes().ToMap()
    ));

    EXPECT_NO_THROW(ParseFormatConfigFromNode(
        CreateAllFieldsSchemaConfig()->Attributes().ToMap()
    ));
}

TEST(TProtobufFormat, TestParseBigZigZag)
{
    constexpr i32 value = Min<i32>();

    TCollectingValueConsumer rowCollector;

    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(CreateAllFieldsSchemaConfig()->Attributes().ToMap()),
        0);
    NProtobufFormatTest::TMessage message;
    message.set_int32_field(value);
    parser->Read(LenvalBytes(message));
    parser->Finish();

    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(0, "Int32")), value);
}

TEST(TProtobufFormat, TestParseEnumerationString)
{
    TCollectingValueConsumer rowCollector;

    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(CreateAllFieldsSchemaConfig()->Attributes().ToMap()),
        0);

    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::one);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::two);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::three);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::minus_forty_two);
        parser->Read(LenvalBytes(message));
    }

    parser->Finish();

    EXPECT_EQ(GetString(rowCollector.GetRowValue(0, "Enum")), "One");
    EXPECT_EQ(GetString(rowCollector.GetRowValue(1, "Enum")), "Two");
    EXPECT_EQ(GetString(rowCollector.GetRowValue(2, "Enum")), "Three");
    EXPECT_EQ(GetString(rowCollector.GetRowValue(3, "Enum")), "MinusFortyTwo");
}

TEST(TProtobufFormat, TestParseWrongEnumeration)
{
    TCollectingValueConsumer rowCollector;

    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(CreateAllFieldsSchemaConfig()->Attributes().ToMap()),
        0);

        NProtobufFormatTest::TMessage message;
        auto enumTag = NProtobufFormatTest::TMessage::descriptor()->FindFieldByName("enum_field")->number();
        message.mutable_unknown_fields()->AddVarint(enumTag, 30);

    auto feedParser = [&] {
        parser->Read(LenvalBytes(message));
        parser->Finish();
    };

    EXPECT_ANY_THROW(feedParser());
}

TEST(TProtobufFormat, TestParseEnumerationInt)
{
    TCollectingValueConsumer rowCollector;

    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                        .Item()
                        .BeginMap()
                            .Item("name").Value("Enum")
                            .Item("field_number").Value(16)
                            .Item("proto_type").Value("enum_int")
                        .EndMap()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    auto parser = CreateParserForProtobuf(&rowCollector, ParseFormatConfigFromNode(config), 0);

    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::one);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::two);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::three);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        message.set_enum_field(NProtobufFormatTest::EEnum::minus_forty_two);
        parser->Read(LenvalBytes(message));
    }
    {
        NProtobufFormatTest::TMessage message;
        auto enumTag = NProtobufFormatTest::TMessage::descriptor()->FindFieldByName("enum_field")->number();
        message.mutable_unknown_fields()->AddVarint(enumTag, 100500);
        parser->Read(LenvalBytes(message));
    }

    parser->Finish();

    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(0, "Enum")), 1);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(1, "Enum")), 2);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(2, "Enum")), 3);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(3, "Enum")), -42);
    EXPECT_EQ(GetInt64(rowCollector.GetRowValue(4, "Enum")), 100500);
}

TEST(TProtobufFormat, TestParseRandomGarbage)
{
    // Check that we never crash.

    TFastRng64 rng(42);
    for (int i = 0; i != 1000; ++i) {
        auto bytes = GenerateRandomLenvalString(rng, 8);

        TCollectingValueConsumer rowCollector;
        auto parser = CreateParserForProtobuf(
            &rowCollector,
            ParseFormatConfigFromNode(CreateAllFieldsSchemaConfig()->Attributes().ToMap()),
            0);
        try {
            parser->Read(bytes);
            parser->Finish();
        } catch (...) {
        }
    }
}

TEST(TProtobufFormat, TestParseZeroColumns)
{
    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    TCollectingValueConsumer rowCollector;
    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(config),
        0);

    // Empty lenval values.
    parser->Read(AsStringBuf("\0\0\0\0"));
    parser->Read(AsStringBuf("\0\0\0\0"));

    parser->Finish();

    ASSERT_EQ(rowCollector.Size(), 2);
    EXPECT_EQ(rowCollector.GetRow(0).GetCount(), 0);
    EXPECT_EQ(rowCollector.GetRow(1).GetCount(), 0);
}

TEST(TProtobufFormat, TestWriteEnumerationString)
{
    auto config = CreateAllFieldsSchemaConfig();

    auto nameTable = New<TNameTable>();
    auto enumId = nameTable->RegisterName("Enum");

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateSchemalessWriterForProtobuf(
        config->Attributes(),
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("MinusFortyTwo", enumId),
        }).Get()
    });
    writer->Write({
        MakeRow({
            MakeUnversionedStringValue("Three", enumId),
        }).Get()
    });

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput si(result);
    TLenvalParser parser(&si);
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        NYT::NProtobufFormatTest::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.enum_field(), NYT::NProtobufFormatTest::EEnum::minus_forty_two);
    }
    {
        auto row = parser.Next();
        ASSERT_TRUE(row);
        NYT::NProtobufFormatTest::TMessage message;
        ASSERT_TRUE(message.ParseFromString(row->RowData));
        ASSERT_EQ(message.enum_field(), NYT::NProtobufFormatTest::EEnum::three);
    }
    {
        auto row = parser.Next();
        ASSERT_FALSE(row);
    }
}

TEST(TProtobufFormat, TestWriteZeroColumns)
{
    auto config = BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndAttributes()
        .Value("protobuf");

    auto nameTable = New<TNameTable>();
    auto int64Id = nameTable->RegisterName("Int64");
    auto stringId = nameTable->RegisterName("String");

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateSchemalessWriterForProtobuf(
        config->Attributes(),
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    writer->Write({
        MakeRow({
            MakeUnversionedInt64Value(-1, int64Id),
            MakeUnversionedStringValue("this_is_string", stringId),
        }).Get()
    });
    writer->Write({MakeRow({ }).Get()});

    writer->Close()
        .Get()
        .ThrowOnError();

    ASSERT_EQ(result, AsStringBuf("\0\0\0\0\0\0\0\0"));
}

TEST(TProtobufFormat, TestContext)
{
    auto config = BuildYsonNodeFluently()
        .BeginMap()
            .Item("tables")
            .BeginList()
                .Item()
                .BeginMap()
                    .Item("columns")
                    .BeginList()
                    .EndList()
                .EndMap()
            .EndList()
        .EndMap();

    TCollectingValueConsumer rowCollector;
    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(config),
        0);

    TString context;
    try {
        NProtobufFormatTest::TMessage message;
        message.set_string_field("PYSHCH-PYSHCH");
        parser->Read(LenvalBytes(message));
        parser->Finish();
        GTEST_FATAL_FAILURE_("expected to throw");
    } catch (const NYT::TErrorException& e) {
        context = *e.Error().Attributes().Find<TString>("context");
    }
    ASSERT_NE(context.find("PYSHCH-PYSHCH"), TString::npos);
}

////////////////////////////////////////////////////////////////////////////////

class TProtobufFormatAllFields
    : public ::testing::TestWithParam<INodePtr>
{ };

INSTANTIATE_TEST_CASE_P(
    Specification,
    TProtobufFormatAllFields,
    ::testing::Values(CreateAllFieldsSchemaConfig()));

INSTANTIATE_TEST_CASE_P(
    FileDescriptor,
    TProtobufFormatAllFields,
    ::testing::Values(CreateAllFieldsFileDescriptorConfig()));

TEST_P(TProtobufFormatAllFields, Writer)
{
    auto config = GetParam();

    auto nameTable = New<TNameTable>();

    auto doubleId = nameTable->RegisterName("Double");
    auto floatId = nameTable->RegisterName("Float");

    auto int64Id = nameTable->RegisterName("Int64");
    auto uint64Id = nameTable->RegisterName("UInt64");
    auto sint64Id = nameTable->RegisterName("SInt64");
    auto fixed64Id = nameTable->RegisterName("Fixed64");
    auto sfixed64Id = nameTable->RegisterName("SFixed64");

    auto int32Id = nameTable->RegisterName("Int32");
    auto uint32Id = nameTable->RegisterName("UInt32");
    auto sint32Id = nameTable->RegisterName("SInt32");
    auto fixed32Id = nameTable->RegisterName("Fixed32");
    auto sfixed32Id = nameTable->RegisterName("SFixed32");

    auto boolId = nameTable->RegisterName("Bool");
    auto stringId = nameTable->RegisterName("String");
    auto bytesId = nameTable->RegisterName("Bytes");

    auto enumId = nameTable->RegisterName("Enum");

    auto messageId = nameTable->RegisterName("Message");

    TString result;
    TStringOutput resultStream(result);
    auto writer = CreateSchemalessWriterForProtobuf(
        config->Attributes(),
        nameTable,
        CreateAsyncAdapter(&resultStream),
        true,
        New<TControlAttributesConfig>(),
        0);

    NProtobufFormatTest::TEmbeddedMessage embeddedMessage;
    embeddedMessage.set_key("embedded_key");
    embeddedMessage.set_value("embedded_value");
    TString embeddedMessageBytes;
    ASSERT_TRUE(embeddedMessage.SerializeToString(&embeddedMessageBytes));

    writer->Write({
        MakeRow({
            MakeUnversionedDoubleValue(3.14159, doubleId),
            MakeUnversionedDoubleValue(2.71828, floatId),

            MakeUnversionedInt64Value(-1, int64Id),
            MakeUnversionedUint64Value(2, uint64Id),
            MakeUnversionedInt64Value(-3, sint64Id),
            MakeUnversionedUint64Value(4, fixed64Id),
            MakeUnversionedInt64Value(-5, sfixed64Id),

            MakeUnversionedInt64Value(-6, int32Id),
            MakeUnversionedUint64Value(7, uint32Id),
            MakeUnversionedInt64Value(-8, sint32Id),
            MakeUnversionedUint64Value(9, fixed32Id),
            MakeUnversionedInt64Value(-10, sfixed32Id),

            MakeUnversionedBooleanValue(true, boolId),
            MakeUnversionedStringValue("this_is_string", stringId),
            MakeUnversionedStringValue("this_is_bytes", bytesId),

            MakeUnversionedStringValue("Two", enumId),

            MakeUnversionedStringValue(embeddedMessageBytes, messageId)
        }).Get()});

    writer->Close()
        .Get()
        .ThrowOnError();

    TStringInput input(result);
    TLenvalParser lenvalParser(&input);

    auto entry = lenvalParser.Next();
    ASSERT_TRUE(entry);

    NYT::NProtobufFormatTest::TMessage message;
    ASSERT_TRUE(message.ParseFromString(entry->RowData));

    EXPECT_DOUBLE_EQ(message.double_field(), 3.14159);
    EXPECT_FLOAT_EQ(message.float_field(), 2.71828);
    EXPECT_EQ(message.int64_field(), -1);
    EXPECT_EQ(message.uint64_field(), 2);
    EXPECT_EQ(message.sint64_field(), -3);
    EXPECT_EQ(message.fixed64_field(), 4);
    EXPECT_EQ(message.sfixed64_field(), -5);

    EXPECT_EQ(message.int32_field(), -6);
    EXPECT_EQ(message.uint32_field(), 7);
    EXPECT_EQ(message.sint32_field(), -8);
    EXPECT_EQ(message.fixed32_field(), 9);
    EXPECT_EQ(message.sfixed32_field(), -10);

    EXPECT_EQ(message.bool_field(), true);
    EXPECT_EQ(message.string_field(), "this_is_string");
    EXPECT_EQ(message.bytes_field(), "this_is_bytes");

    EXPECT_EQ(message.enum_field(), NProtobufFormatTest::EEnum::two);

    EXPECT_EQ(message.message_field().key(), "embedded_key");
    EXPECT_EQ(message.message_field().value(), "embedded_value");

    ASSERT_FALSE(lenvalParser.Next());
}

TEST_P(TProtobufFormatAllFields, Parser)
{
    TCollectingValueConsumer rowCollector;

    auto parser = CreateParserForProtobuf(
        &rowCollector,
        ParseFormatConfigFromNode(GetParam()->Attributes().ToMap()),
        0);

    NProtobufFormatTest::TMessage message;
    message.set_double_field(3.14159);
    message.set_float_field(2.71828);

    message.set_int64_field(-1);
    message.set_uint64_field(2);
    message.set_sint64_field(-3);
    message.set_fixed64_field(4);
    message.set_sfixed64_field(-5);

    message.set_int32_field(-6);
    message.set_uint32_field(7);
    message.set_sint32_field(-8);
    message.set_fixed32_field(9);
    message.set_sfixed32_field(-10);

    message.set_bool_field(true);
    message.set_string_field("this_is_string");
    message.set_bytes_field("this_is_bytes");
    message.set_enum_field(NProtobufFormatTest::EEnum::three);

    message.mutable_message_field()->set_key("embedded_key");
    message.mutable_message_field()->set_value("embedded_value");

    TString lenvalBytes;
    {
        TStringOutput out(lenvalBytes);
        ui32 messageSize = message.ByteSize();
        out.Write(&messageSize, sizeof(messageSize));
        ASSERT_TRUE(message.SerializeToStream(&out));
    }

    parser->Read(lenvalBytes);
    parser->Finish();

    ASSERT_EQ(rowCollector.Size(), 1);

    ASSERT_DOUBLE_EQ(GetDouble(rowCollector.GetRowValue(0, "Double")), 3.14159);
    ASSERT_NEAR(GetDouble(rowCollector.GetRowValue(0, "Float")), 2.71828, 1e-5);

    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "Int64")), -1);
    ASSERT_EQ(GetUint64(rowCollector.GetRowValue(0, "UInt64")), 2);
    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "SInt64")), -3);
    ASSERT_EQ(GetUint64(rowCollector.GetRowValue(0, "Fixed64")), 4);
    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "SFixed64")), -5);

    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "Int32")), -6);
    ASSERT_EQ(GetUint64(rowCollector.GetRowValue(0, "UInt32")), 7);
    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "SInt32")), -8);
    ASSERT_EQ(GetUint64(rowCollector.GetRowValue(0, "Fixed32")), 9);
    ASSERT_EQ(GetInt64(rowCollector.GetRowValue(0, "SFixed32")), -10);

    ASSERT_EQ(GetBoolean(rowCollector.GetRowValue(0, "Bool")), true);
    ASSERT_EQ(GetString(rowCollector.GetRowValue(0, "String")), "this_is_string");
    ASSERT_EQ(GetString(rowCollector.GetRowValue(0, "Bytes")), "this_is_bytes");

    NProtobufFormatTest::TEmbeddedMessage embededMessage;
    ASSERT_TRUE(embededMessage.ParseFromString(GetString(rowCollector.GetRowValue(0, "Message"))));
    ASSERT_EQ(embededMessage.key(), "embedded_key");
    ASSERT_EQ(embededMessage.value(), "embedded_value");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
