#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/all_types_proto3.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/clashing_enums.pb.h>
#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <yt/cpp/mapreduce/http/abortable_http_response.h>

#include <yt/cpp/mapreduce/io/proto_table_reader.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/scope.h>

#include <type_traits>

using namespace NYT;
using namespace NYT::NTesting;

template <typename TRow>
static NTi::TTypePtr GetRowType();

template <>
NTi::TTypePtr GetRowType<TUrlRow>()
{
    static auto type = NTi::Struct({
        {"Host", NTi::String()},
        {"Path", NTi::String()},
        {"HttpCode", NTi::Int32()},
    });
    return type;
}

template <>
NTi::TTypePtr GetRowType<TUrlRowWithColumnNames>()
{
    static auto type = NTi::Struct({
        {"Host_ColumnName", NTi::String()},
        {"Path_KeyColumnName", NTi::String()},
        {"http_code", NTi::Int32()},
    });
    return type;
}

template <>
void Out<TRowWithTypeOptions_Color>(IOutputStream& o, TRowWithTypeOptions_Color color)
{
    o << TRowWithTypeOptions::Color_Name(color);
}

class TProtobufTableIoTest
    : public ::testing::TestWithParam<bool>
{ };

TEST_P(TProtobufTableIoTest, ReadingWritingProtobufAllTypesProto3)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto path = TRichYPath(workingDir + "/proto_table");
    TAllTypesMessageProto3 message;
    message.set_double_field(42.4242);
    message.set_float_field(3.14159);
    message.set_int64_field(-4200);
    // OmittedInt64Field is not set deliberately.
    message.set_uint64_field(4200);
    message.set_sint64_field(-4242);
    message.set_fixed64_field(432101234);
    message.set_sfixed64_field(41112222);
    message.set_int32_field(-3124232);
    message.set_uint32_field(12321342);
    message.set_sint32_field(-42442);
    message.set_fixed32_field(2134242);
    message.set_sfixed32_field(422142);
    message.set_bool_field(true);
    message.set_string_field("42");
    message.set_bytes_field("36 popugayev");
    message.set_enum_field(EEnumProto3::OneProto3);
    message.mutable_message_field()->set_key("key");
    message.mutable_message_field()->set_value("value");

    {
        auto writer = client->CreateTableWriter<TAllTypesMessageProto3>(path);
        writer->AddRow(message);
        writer->Finish();
    }
    {
        auto reader = client->CreateTableReader<TAllTypesMessageProto3>(path);
        EXPECT_TRUE(reader->IsValid());
        const auto& row = reader->GetRow();
        ASSERT_NEAR(message.double_field(), row.double_field(), 1e-6);
        ASSERT_NEAR(message.float_field(), row.float_field(), 1e-6);
        EXPECT_EQ(message.int64_field(), row.int64_field());
        EXPECT_EQ(message.omitted_int64_field(), row.omitted_int64_field());
        EXPECT_EQ(message.omitted_int64_field(), 0);
        EXPECT_EQ(message.uint64_field(), row.uint64_field());
        EXPECT_EQ(message.sint64_field(), row.sint64_field());
        EXPECT_EQ(message.fixed64_field(), row.fixed64_field());
        EXPECT_EQ(message.sfixed64_field(), row.sfixed64_field());
        EXPECT_EQ(message.int32_field(), row.int32_field());
        EXPECT_EQ(message.uint32_field(), row.uint32_field());
        EXPECT_EQ(message.sint32_field(), row.sint32_field());
        EXPECT_EQ(message.fixed32_field(), row.fixed32_field());
        EXPECT_EQ(message.sfixed32_field(), row.sfixed32_field());
        EXPECT_EQ(message.bool_field(), row.bool_field());
        EXPECT_EQ(message.string_field(), row.string_field());
        EXPECT_EQ(message.bytes_field(), row.bytes_field());
        EXPECT_EQ(message.enum_field(), row.enum_field());
        EXPECT_EQ(message.message_field().key(), row.message_field().key());
        EXPECT_EQ(message.message_field().value(), row.message_field().value());
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }
}

TEST_P(TProtobufTableIoTest, ReadingWritingProtobufAllTypes)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto path = TRichYPath(workingDir + "/proto_table");
    TAllTypesMessage message;
    message.set_double_field(42.4242);
    message.set_float_field(3.14159);
    message.set_int64_field(-4200);
    message.set_uint64_field(4200);
    message.set_sint64_field(-4242);
    message.set_fixed64_field(432101234);
    message.set_sfixed64_field(41112222);
    message.set_int32_field(-3124232);
    message.set_uint32_field(12321342);
    message.set_sint32_field(-42442);
    message.set_fixed32_field(2134242);
    message.set_sfixed32_field(422142);
    message.set_bool_field(true);
    message.set_string_field("42");
    message.set_bytes_field("36 popugayev");
    message.set_enum_field(EEnum::One);
    message.mutable_message_field()->set_key("key");
    message.mutable_message_field()->set_value("value");

    {
        auto writer = client->CreateTableWriter<TAllTypesMessage>(path);
        writer->AddRow(message);
        writer->Finish();
    }
    {
        auto reader = client->CreateTableReader<TAllTypesMessage>(path);
        EXPECT_TRUE(reader->IsValid());
        const auto& row = reader->GetRow();
        ASSERT_NEAR(message.double_field(), row.double_field(), 1e-6);
        ASSERT_NEAR(message.float_field(), row.float_field(), 1e-6);
        EXPECT_EQ(message.int64_field(), row.int64_field());
        EXPECT_EQ(message.uint64_field(), row.uint64_field());
        EXPECT_EQ(message.sint64_field(), row.sint64_field());
        EXPECT_EQ(message.fixed64_field(), row.fixed64_field());
        EXPECT_EQ(message.sfixed64_field(), row.sfixed64_field());
        EXPECT_EQ(message.int32_field(), row.int32_field());
        EXPECT_EQ(message.uint32_field(), row.uint32_field());
        EXPECT_EQ(message.sint32_field(), row.sint32_field());
        EXPECT_EQ(message.fixed32_field(), row.fixed32_field());
        EXPECT_EQ(message.sfixed32_field(), row.sfixed32_field());
        EXPECT_EQ(message.bool_field(), row.bool_field());
        EXPECT_EQ(message.string_field(), row.string_field());
        EXPECT_EQ(message.bytes_field(), row.bytes_field());
        EXPECT_EQ(message.enum_field(), row.enum_field());
        EXPECT_EQ(message.message_field().key(), row.message_field().key());
        EXPECT_EQ(message.message_field().value(), row.message_field().value());
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }
}

TEST_P(TProtobufTableIoTest, UntypedProtobufWriter)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    {
        TUrlRow row;
        row.set_host("http://www.example.com");
        row.set_path("/index.php");
        row.set_http_code(200);
        const Message* ptrWithoutType = &row;

        auto writer = client->CreateTableWriter(workingDir + "/urls", *TUrlRow::descriptor());
        writer->AddRow(*ptrWithoutType);
        writer->Finish();
    }

    auto reader = client->CreateTableReader<TUrlRow>(workingDir + "/urls");
    EXPECT_TRUE(reader->IsValid());
    {
        const auto& row = reader->GetRow();
        EXPECT_EQ(row.host(), "http://www.example.com");
        EXPECT_EQ(row.path(), "/index.php");
        EXPECT_EQ(row.http_code(), 200);
    }
    reader->Next();
    EXPECT_TRUE(!reader->IsValid());
}

TEST_P(TProtobufTableIoTest, ProtobufVersions)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    {
        const auto writer = client->CreateTableWriter<TRowVer1>(workingDir + "/ver1");
        TRowVer1 data;
        data.set_string_1("Ver1_String_1");
        data.set_uint32_2(0x12);
        writer->AddRow(data);
        writer->Finish();
    }

    //V1 as V2
    {
        const auto reader = client->CreateTableReader<TRowVer2>(workingDir + "/ver1");
        EXPECT_TRUE(reader->IsValid());
        {
            const auto& data = reader->GetRow();
            EXPECT_TRUE(data.has_string_1());
            EXPECT_EQ(data.string_1(), "Ver1_String_1");
            EXPECT_TRUE(data.has_uint32_2());
            EXPECT_EQ(data.uint32_2(), static_cast<ui32>(0x12));
            EXPECT_TRUE(!data.has_fixed64_3());
            EXPECT_EQ(data.unknown_fields().field_count(), 0);
        }
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }

    {
        const auto writer = client->CreateTableWriter<TRowVer2>(workingDir + "/ver2");
        TRowVer2 data;
        data.set_string_1("Ver2_String_1");
        data.set_uint32_2(0x22);
        data.set_fixed64_3(0x23);
        writer->AddRow(data);
        writer->Finish();
    }

    //V2 as V1
    {
        const auto reader = client->CreateTableReader<TRowVer1>(workingDir + "/ver2");
        EXPECT_TRUE(reader->IsValid());
        {
            const auto& data = reader->GetRow();
            EXPECT_TRUE(data.has_string_1());
            EXPECT_EQ(data.string_1(), "Ver2_String_1");
            EXPECT_TRUE(data.has_uint32_2());
            EXPECT_EQ(data.uint32_2(), static_cast<ui32>(0x22));
            //no unknown fields supported
            EXPECT_EQ(data.unknown_fields().field_count(), 0);
        }
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }
}

struct TTestColumnNames
{
    TString Host;
    TString Path;
    TString HttpCode;
};

template <typename TRow, typename TElement>
void TestProtobufSerializationModes(
    EWrapperFieldFlag::Enum mode1,
    EWrapperFieldFlag::Enum mode2,
    bool WithDescriptors,
    const TTestColumnNames& columnNames = {"Host", "Path", "HttpCode"})
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = WithDescriptors;

    auto setType = [] (EWrapperFieldFlag::Enum mode, TColumnSchema& column) {
        switch (mode) {
            case EWrapperFieldFlag::SERIALIZATION_YT:
                column.Type(GetRowType<TElement>());
                return;
            case EWrapperFieldFlag::SERIALIZATION_PROTOBUF:
                column.Type(EValueType::VT_STRING);
                return;
            default:
                Y_ABORT();
        }
    };

    auto schema = TTableSchema();
    auto column1 = TColumnSchema().Name("url_row_1");
    setType(mode1, column1);
    auto column2 = TColumnSchema().Name("url_row_2");
    setType(mode2, column2);
    schema.AddColumn(column1).AddColumn(column2);

    TVector<std::tuple<TString, TString, int>> firstValuesInRows{
        {"http://www.example.com", "/", 302},
        {"http://www.example.com", "/index.php", 200},
    };

    TVector<TVector<TString>> protobufSerializedValues;
    TVector<TVector<TNode>> nodeSerializedValues;
    {
        auto writer = client->CreateTableWriter<TRow>(
            TRichYPath(workingDir + "/table").Schema(schema));
        for (const auto& [host, path, code] : firstValuesInRows) {
            TRow row;
            auto& embedded1 = *row.mutable_url_row_1();
            embedded1.set_host(host);
            embedded1.set_path(path);
            embedded1.set_http_code(code);
            auto& embedded2 = *row.mutable_url_row_2();
            embedded2.set_host(host);
            embedded2.set_path(path);
            embedded2.set_http_code(code + 1);
            writer->AddRow(row);

            TString embedded1Serialized = SerializeProtoToString(embedded1);
            TString embedded2Serialized = SerializeProtoToString(embedded2);
            protobufSerializedValues.push_back(TVector<TString>{FromProto<TString>(embedded1Serialized), FromProto<TString>(embedded2Serialized)});

            nodeSerializedValues.push_back(TVector<TNode>{
                TNode()(columnNames.Host, host)(columnNames.Path, path)(columnNames.HttpCode, code),
                TNode()(columnNames.Host, host)(columnNames.Path, path)(columnNames.HttpCode, code + 1),
            });
        }
        writer->Finish();
    }

    auto checkValue = [&] (const TNode& value, EWrapperFieldFlag::Enum mode, int rowIndex, int valueIndex) {
        switch (mode) {
            case EWrapperFieldFlag::SERIALIZATION_YT:
                EXPECT_EQ(value, nodeSerializedValues[rowIndex][valueIndex]);
                return;
            case EWrapperFieldFlag::SERIALIZATION_PROTOBUF:
                EXPECT_EQ(value.GetType(), TNode::EType::String);
                EXPECT_EQ(value.AsString(), protobufSerializedValues[rowIndex][valueIndex]);
                return;
            default:
                Y_ABORT();
        }
    };

    // Check that all the values were written correctly.
    {
        auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
        EXPECT_TRUE(reader->IsValid());
        EXPECT_TRUE(reader->GetRow().HasKey("url_row_1"));
        EXPECT_TRUE(reader->GetRow().HasKey("url_row_2"));
        checkValue(reader->GetRow()["url_row_1"], mode1, 0, 0);
        checkValue(reader->GetRow()["url_row_2"], mode2, 0, 1);
        reader->Next();
        EXPECT_TRUE(reader->IsValid());
        EXPECT_TRUE(reader->GetRow().HasKey("url_row_1"));
        EXPECT_TRUE(reader->GetRow().HasKey("url_row_2"));
        checkValue(reader->GetRow()["url_row_1"], mode1, 1, 0);
        checkValue(reader->GetRow()["url_row_2"], mode2, 1, 1);
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }

    // Check that all the values can be read by protobuf reader.
    auto reader = client->CreateTableReader<TRow>(workingDir + "/table");
    for (const auto& [host, path, code] : firstValuesInRows) {
        EXPECT_TRUE(reader->IsValid());
        const auto& row = reader->GetRow();
        const auto& embedded1 = row.url_row_1();
        EXPECT_EQ(embedded1.host(), host);
        EXPECT_EQ(embedded1.path(), path);
        EXPECT_EQ(embedded1.http_code(), code);
        const auto& embedded2 = row.url_row_2();
        EXPECT_EQ(embedded2.host(), host);
        EXPECT_EQ(embedded2.path(), path);
        EXPECT_EQ(embedded2.http_code(), code + 1);
        reader->Next();
    }
    EXPECT_TRUE(!reader->IsValid());
}

TEST_P(TProtobufTableIoTest, ProtobufSerializationMode_FieldOption)
{
    TestProtobufSerializationModes<TRowFieldSerializationOption, TUrlRow>(
        EWrapperFieldFlag::SERIALIZATION_YT,
        EWrapperFieldFlag::SERIALIZATION_PROTOBUF,
        GetParam());
}

TEST_P(TProtobufTableIoTest, ProtobufSerializationMode_MessageOption)
{
    TestProtobufSerializationModes<TRowMessageSerializationOption, TUrlRow>(
        EWrapperFieldFlag::SERIALIZATION_YT,
        EWrapperFieldFlag::SERIALIZATION_YT,
        GetParam());
}

TEST_P(TProtobufTableIoTest, ProtobufSerializationMode_MixedOptions)
{
    TestProtobufSerializationModes<TRowMixedSerializationOptions, TUrlRow>(
        EWrapperFieldFlag::SERIALIZATION_YT,
        EWrapperFieldFlag::SERIALIZATION_PROTOBUF,
        GetParam());
}

TEST_P(TProtobufTableIoTest, ProtobufSerializationMode_MixedOptions_ColumnNames)
{
    TestProtobufSerializationModes<TRowMixedSerializationOptions_ColumnNames, TUrlRowWithColumnNames>(
        EWrapperFieldFlag::SERIALIZATION_YT,
        EWrapperFieldFlag::SERIALIZATION_PROTOBUF,
        GetParam(),
        {"Host_ColumnName", "Path_KeyColumnName", "http_code"});
}

TEST_P(TProtobufTableIoTest, ProtobufRepeatedSerialization)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("ints").Type(NTi::List(NTi::Int64())))
        .AddColumn(TColumnSchema().Name("url_rows").Type(NTi::List(GetRowType<TUrlRow>())))
        .AddColumn(TColumnSchema().Name("packed_ints").Type(NTi::List(NTi::Int64())));

    {
        auto writer = client->CreateTableWriter<TRowSerializedRepeatedFields>(
            TRichYPath(workingDir + "/table").Schema(schema));
        {
            TRowSerializedRepeatedFields row;
            row.add_ints(1);
            row.add_ints(2);
            row.add_ints(3);
            row.add_packed_ints(-1);
            row.add_packed_ints(-2);
            row.add_packed_ints(-3);
            auto& urlRow1 = *row.add_url_rows();
            urlRow1.set_host("http://www.example.com");
            urlRow1.set_path("/");
            urlRow1.set_http_code(303);
            auto& urlRow2 = *row.add_url_rows();
            urlRow2.set_host("http://www.example.com");
            urlRow2.set_path("/");
            urlRow2.set_http_code(307);
            writer->AddRow(row);
        }
        {
            TRowSerializedRepeatedFields row;
            row.add_ints(101);
            row.add_ints(201);
            row.add_ints(301);
            row.add_packed_ints(-101);
            row.add_packed_ints(-201);
            row.add_packed_ints(-301);
            auto& urlRow1 = *row.add_url_rows();
            urlRow1.set_host("http://www.example.com");
            urlRow1.set_path("/index.php");
            urlRow1.set_http_code(200);
            auto& urlRow2 = *row.add_url_rows();
            urlRow2.set_host("http://www.example.com");
            urlRow2.set_path("/index.php");
            urlRow2.set_http_code(201);
            writer->AddRow(row);
        }
        writer->Finish();
    }

    {
        auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
        EXPECT_TRUE(reader->IsValid());
        EXPECT_EQ(
            reader->GetRow(),
            TNode()
                ("ints", TNode().Add(1).Add(2).Add(3))
                ("packed_ints", TNode().Add(-1).Add(-2).Add(-3))
                ("url_rows", TNode()
                    .Add(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 303))
                    .Add(TNode()("Host", "http://www.example.com")("Path", "/")("HttpCode", 307))));
        reader->Next();
        EXPECT_TRUE(reader->IsValid());
        EXPECT_EQ(
            reader->GetRow(),
            TNode()
                ("ints", TNode().Add(101).Add(201).Add(301))
                ("packed_ints", TNode().Add(-101).Add(-201).Add(-301))
                ("url_rows", TNode()
                    .Add(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 200))
                    .Add(TNode()("Host", "http://www.example.com")("Path", "/index.php")("HttpCode", 201))));
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }

    auto reader = client->CreateTableReader<TRowSerializedRepeatedFields>(workingDir + "/table");
    EXPECT_TRUE(reader->IsValid());
    {
        const auto& row = reader->GetRow();
        TVector<int> ints(row.ints().begin(), row.ints().end());
        EXPECT_EQ(ints, (TVector<int>{1, 2, 3}));
        TVector<int> packedInts(row.packed_ints().begin(), row.packed_ints().end());
        EXPECT_EQ(packedInts, (TVector<int>{-1, -2, -3}));
        EXPECT_EQ(static_cast<int>(row.url_rows_size()), 2);
        const auto& urlRow1 = row.url_rows(0);
        EXPECT_EQ(urlRow1.host(), "http://www.example.com");
        EXPECT_EQ(urlRow1.path(), "/");
        EXPECT_EQ(urlRow1.http_code(), 303);
        const auto& urlRow2 = row.url_rows(1);
        EXPECT_EQ(urlRow2.host(), "http://www.example.com");
        EXPECT_EQ(urlRow2.path(), "/");
        EXPECT_EQ(urlRow2.http_code(), 307);
    }
    reader->Next();
    EXPECT_TRUE(reader->IsValid());
    {
        const auto& row = reader->GetRow();
        TVector<int> ints(row.ints().begin(), row.ints().end());
        EXPECT_EQ(ints, (TVector<int>{101, 201, 301}));
        TVector<int> packedInts(row.packed_ints().begin(), row.packed_ints().end());
        EXPECT_EQ(packedInts, (TVector<int>{-101, -201, -301}));
        EXPECT_EQ(static_cast<int>(row.url_rows_size()), 2);
        const auto& urlRow1 = row.url_rows(0);
        EXPECT_EQ(urlRow1.host(), "http://www.example.com");
        EXPECT_EQ(urlRow1.path(), "/index.php");
        EXPECT_EQ(urlRow1.http_code(), 200);
        const auto& urlRow2 = row.url_rows(1);
        EXPECT_EQ(urlRow2.host(), "http://www.example.com");
        EXPECT_EQ(urlRow2.path(), "/index.php");
        EXPECT_EQ(urlRow2.http_code(), 201);
    }
    reader->Next();
    EXPECT_TRUE(!reader->IsValid());
}

TEST_P(TProtobufTableIoTest, ForbiddenRepeatedInProtobuf)
{
    SKIP_IF_RPC();

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto schema = TTableSchema()
        .AddColumn(TColumnSchema().Name("ints").Type(NTi::List(NTi::Int64())));

    auto run = [&] {
        auto writer = client->CreateTableWriter<TBadProtobufSerializedRow>(
            TRichYPath(workingDir + "/table").Schema(schema));
        writer->Finish();
    };
    EXPECT_THROW_MESSAGE_HAS_SUBSTR(
        run(),
        yexception,
        "Repeated field \"NYT.NTesting.TBadProtobufSerializedRow.ints\" must have flag \"SERIALIZATION_YT\"");
}

TEST_P(TProtobufTableIoTest, ProtobufWithTypeOption)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto schema = TTableSchema()
        .Strict(false)
        .AddColumn(TColumnSchema()
            .Name("color_int_field").Type(EValueType::VT_INT64))
        .AddColumn(TColumnSchema()
            .Name("color_string_field").Type(EValueType::VT_STRING))
        .AddColumn(TColumnSchema()
            .Name("any_field").Type(EValueType::VT_ANY))
        .AddColumn(TColumnSchema()
            .Name("embedded_field").RawTypeV3(TNode()
                ("type_name", "optional")
                ("item", TNode()
                    ("type_name", "struct")
                    ("members", TNode()
                        .Add(TNode()
                            ("name", "color_int_field")
                            ("type", "int64"))
                        .Add(TNode()
                            ("name", "color_string_field")
                            ("type", "string"))
                        .Add(TNode()
                            ("name", "any_field")
                            ("type", TNode()
                                ("type_name", "optional")
                                ("item", "yson")))))))
        .AddColumn(TColumnSchema()
            .Name("repeated_enum_int_field").RawTypeV3(TNode()
                ("type_name", "list")
                ("item", "int64")))
        .AddColumn(TColumnSchema()
            .Name("unknown_schematized_column").Type(EValueType::VT_BOOLEAN));

    auto node1 = TNode()("x", TNode()("y", 12));
    auto node2 = TNode()("key", "value");
    auto node3 = TNode().Add(1).Add("string").Add(true);
    auto node4 = TNode("hooray");

    {
        auto writer = client->CreateTableWriter<TRowWithTypeOptions>(
            TRichYPath(workingDir + "/table").Schema(schema));
        {
            TRowWithTypeOptions row;
            row.set_color_int_field(TRowWithTypeOptions::RED);
            row.set_color_string_field(TRowWithTypeOptions::BLUE);
            row.set_any_field(NodeToYsonString(node1));
            row.set_other_columns_field(NodeToYsonString(
                TNode()
                    ("unknown_schematized_column", true)
                    ("unknown_unschematized_column", 1234)));
            auto& embedded = *row.mutable_embedded_field();
            embedded.set_color_int_field(TRowWithTypeOptions::WHITE);
            embedded.set_color_string_field(TRowWithTypeOptions::RED);
            embedded.set_any_field(NodeToYsonString(node2));
            row.add_repeated_enum_int_field(TRowWithTypeOptions::WHITE);
            row.add_repeated_enum_int_field(TRowWithTypeOptions::BLUE);
            row.add_repeated_enum_int_field(TRowWithTypeOptions::RED);
            writer->AddRow(row);
        }
        {
            TRowWithTypeOptions row;
            row.set_color_int_field(TRowWithTypeOptions::WHITE);
            row.set_color_string_field(TRowWithTypeOptions::RED);
            row.set_any_field(NodeToYsonString(node3));
            row.set_other_columns_field(NodeToYsonString(
                TNode()
                    ("unknown_schematized_column", false)
                    ("unknown_unschematized_column", "some-string")));
            auto& embedded = *row.mutable_embedded_field();
            embedded.set_color_int_field(TRowWithTypeOptions::RED);
            embedded.set_color_string_field(TRowWithTypeOptions::WHITE);
            embedded.set_any_field(NodeToYsonString(node4));
            row.add_repeated_enum_int_field(TRowWithTypeOptions::BLUE);
            writer->AddRow(row);
        }
        writer->Finish();
    }

    {
        auto reader = client->CreateTableReader<TNode>(workingDir + "/table");
        EXPECT_TRUE(reader->IsValid());
        EXPECT_EQ(
            reader->GetRow(),
            TNode()
                ("color_int_field", -1)
                ("color_string_field", "BLUE")
                ("any_field", node1)
                ("unknown_schematized_column", true)
                ("unknown_unschematized_column", 1234)
                ("embedded_field", TNode()
                    ("color_int_field", 0)
                    ("color_string_field", "RED")
                    ("any_field", node2))
                ("repeated_enum_int_field", TNode()
                    .Add(0)
                    .Add(1)
                    .Add(-1)));
        reader->Next();
        EXPECT_TRUE(reader->IsValid());
        EXPECT_EQ(
            reader->GetRow(),
            TNode()
                ("color_int_field", 0)
                ("color_string_field", "RED")
                ("any_field", node3)
                ("unknown_schematized_column", false)
                ("unknown_unschematized_column", "some-string")
                ("embedded_field", TNode()
                    ("color_int_field", -1)
                    ("color_string_field", "WHITE")
                    ("any_field", node4))
                ("repeated_enum_int_field", TNode().Add(1)));
        reader->Next();
        EXPECT_TRUE(!reader->IsValid());
    }

    auto reader = client->CreateTableReader<TRowWithTypeOptions>(workingDir + "/table");
    EXPECT_TRUE(reader->IsValid());
    {
        const auto& row = reader->GetRow();
        EXPECT_EQ(row.color_int_field(), TRowWithTypeOptions::RED);
        EXPECT_EQ(row.color_string_field(), TRowWithTypeOptions::BLUE);
        EXPECT_EQ(NodeFromYsonString(row.any_field()), node1);
        auto otherColumns = NodeFromYsonString(row.other_columns_field());
        EXPECT_TRUE(otherColumns.HasKey("unknown_schematized_column"));
        EXPECT_EQ(otherColumns["unknown_schematized_column"], true);
        EXPECT_TRUE(otherColumns.HasKey("unknown_unschematized_column"));
        EXPECT_EQ(otherColumns["unknown_unschematized_column"], 1234);
        const auto& embedded = row.embedded_field();
        EXPECT_EQ(embedded.color_int_field(), TRowWithTypeOptions::WHITE);
        EXPECT_EQ(embedded.color_string_field(), TRowWithTypeOptions::RED);
        EXPECT_EQ(NodeFromYsonString(embedded.any_field()), node2);

        TVector<TRowWithTypeOptions::Color> colors;
        for (auto intColor : row.repeated_enum_int_field()) {
            colors.push_back(static_cast<TRowWithTypeOptions::Color>(intColor));
        }
        EXPECT_EQ(
            colors,
            (TVector<TRowWithTypeOptions::Color>{
                TRowWithTypeOptions::WHITE,
                TRowWithTypeOptions::BLUE,
                TRowWithTypeOptions::RED,
            }));
    }
    reader->Next();
    EXPECT_TRUE(reader->IsValid());
    {
        const auto& row = reader->GetRow();
        EXPECT_EQ(row.color_int_field(), TRowWithTypeOptions::WHITE);
        EXPECT_EQ(row.color_string_field(), TRowWithTypeOptions::RED);
        EXPECT_EQ(NodeFromYsonString(row.any_field()), node3);
        auto otherColumns = NodeFromYsonString(row.other_columns_field());
        EXPECT_TRUE(otherColumns.HasKey("unknown_schematized_column"));
        EXPECT_EQ(otherColumns["unknown_schematized_column"], false);
        EXPECT_TRUE(otherColumns.HasKey("unknown_unschematized_column"));
        EXPECT_EQ(otherColumns["unknown_unschematized_column"], "some-string");
        const auto& embedded = row.embedded_field();
        EXPECT_EQ(embedded.color_int_field(), TRowWithTypeOptions::RED);
        EXPECT_EQ(embedded.color_string_field(), TRowWithTypeOptions::WHITE);
        EXPECT_EQ(NodeFromYsonString(embedded.any_field()), node4);

        TVector<TRowWithTypeOptions::Color> colors;
        for (auto intColor : row.repeated_enum_int_field()) {
            colors.push_back(static_cast<TRowWithTypeOptions::Color>(intColor));
        }
        EXPECT_EQ(
            colors,
            (TVector<TRowWithTypeOptions::Color>{TRowWithTypeOptions::BLUE}));
    }
    reader->Next();
    EXPECT_TRUE(!reader->IsValid());
}

void TestProtobufSchemaInferring(bool setWriterOptions, bool WithDescriptors)
{
    TTableWriterOptions options;
    TConfigSaverGuard configGuard;
    TConfig::Get()->ProtobufFormatWithDescriptors = WithDescriptors;

    if (setWriterOptions) {
        options.InferSchema(true);
    } else {
        TConfig::Get()->InferTableSchema = true;
    }

    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();

    {
        auto writer = client->CreateTableWriter<TRowVer2>(workingDir + "/table", options);
        TRowVer2 row;
        row.set_string_1("abc");
        row.set_uint32_2(40 + 2);
        row.set_fixed64_3(8888);
        writer->AddRow(row);
        writer->Finish();
    }

    TTableSchema actualSchema;
    Deserialize(actualSchema, client->Get(workingDir + "/table/@schema"));
    EXPECT_TRUE(AreSchemasEqual(
        actualSchema,
        TTableSchema()
            .AddColumn(TColumnSchema().Name("string_1").Type(EValueType::VT_STRING))
            .AddColumn(TColumnSchema().Name("uint32_2").Type(EValueType::VT_UINT32))
            .AddColumn(TColumnSchema().Name("fixed64_3").Type(EValueType::VT_UINT64))));
}

TEST_P(TProtobufTableIoTest, ProtobufSchemaInferring_Config)
{
    TestProtobufSchemaInferring(false, GetParam());
}

TEST_P(TProtobufTableIoTest, ProtobufSchemaInferring_Options)
{
    TestProtobufSchemaInferring(true, GetParam());
}

TEST_P(TProtobufTableIoTest, ProtobufWriteRead_Enum)
{
    TTestFixture fixture;
    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();
    TConfig::Get()->UseClientProtobuf = false;

    auto table = TRichYPath(workingDir + "/table");

    const TVector<EEnum> expected{EEnum::One, EEnum::Two, EEnum::Three, EEnum::MinusFortyTwo};
    {
        auto writer = client->CreateTableWriter<TAllTypesMessage>(table);

        for (const EEnum& enumField : expected) {
            TAllTypesMessage row;
            row.set_enum_field(enumField);
            row.set_enum_int_field(enumField);
            writer->AddRow(row);
        }

        writer->Finish();
    }

    TVector<EEnum> actual;
    {
        auto reader = client->CreateTableReader<TNode>(table);

        for (; reader->IsValid(); reader->Next()) {
            TAllTypesMessage row;
            ReadMessageFromNode(reader->GetRow(), &row);

            EXPECT_EQ(row.enum_field(), row.enum_int_field());

            actual.push_back(row.enum_field());
        }
    }

    EXPECT_EQ(expected.size(), actual.size());
    for (size_t i = 0; i < actual.size(); ++i) {
        EXPECT_EQ(expected[i], actual[i]);
    }
}

TEST_P(TProtobufTableIoTest, ProtoClashingEnums_YT_13714)
{
    TTestFixture fixture;
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto testTable = workingDir + "/table";

    TClashingEnumMessage originalRow;
    originalRow.set_enum1(TClashingEnumMessage1::ClashingEnumValueOne);
    originalRow.set_enum2(TClashingEnumMessage2::ClashingEnumValueTwo);
    originalRow.set_enum3(ClashingEnumValueThree);
    {
        auto writer = client->CreateTableWriter<TClashingEnumMessage>(testTable);
        writer->AddRow(originalRow);
        writer->Finish();
    }

    TVector<TClashingEnumMessage> readValues;
    auto reader = client->CreateTableReader<TClashingEnumMessage>(testTable);
    for (const auto& cursor : *reader) {
        readValues.push_back(cursor.GetRow());
    }

    EXPECT_EQ(std::ssize(readValues), 1);
    EXPECT_EQ(readValues[0].ShortDebugString(), originalRow.ShortDebugString());
}

TEST_P(TProtobufTableIoTest, Proto3Optional)
{
    TTestFixture fixture;
    TConfig::Get()->ProtobufFormatWithDescriptors = GetParam();

    // TODO(levysotsky): Enable this test after YT-15121 is deployed.
    if (GetParam()) {
        return;
    }

    auto client = fixture.GetClient();
    auto workingDir = fixture.GetWorkingDir();
    auto testTable = workingDir + "/table";

    TWithOptional row;
    row.set_optional_field(42);
    row.set_non_optional_field(12);

    {
        auto writer = client->CreateTableWriter<TWithOptional>(
            TRichYPath(testTable)
                .Schema(CreateTableSchema<TWithOptional>())
        );
        writer->AddRow(row);
        writer->Finish();
    }

    TTableSchema schema;
    Deserialize(schema, client->Get(testTable + "/@schema"));

    EXPECT_EQ(schema, TTableSchema()
        .AddColumn(TColumnSchema().Name("optional_field").Type(NTi::Optional(NTi::Int64())))
        .AddColumn(TColumnSchema().Name("Dummy").Type(
            NTi::Optional(
                NTi::Variant(
                    NTi::Struct({
                        {"_fieldInsideOneof", NTi::Int64()},
                    })
                )
            )
        ))
        .AddColumn(TColumnSchema().Name("embedded_field").Type(
            NTi::Optional(
                NTi::Struct({
                    {"optional_field", NTi::Optional(NTi::Int64())},
                })
            )
        ))
        .AddColumn(TColumnSchema().Name("non_optional_field").Type(NTi::Optional(NTi::Int64())))
    );

    TVector<TWithOptional> readValues;
    auto reader = client->CreateTableReader<TWithOptional>(testTable);
    for (const auto& cursor : *reader) {
        readValues.push_back(cursor.GetRow());
    }

    EXPECT_EQ(std::ssize(readValues), 1);
    EXPECT_EQ(readValues[0].ShortDebugString(), row.ShortDebugString());
}

INSTANTIATE_TEST_SUITE_P(WithDescriptors, TProtobufTableIoTest, ::testing::Values(true));
INSTANTIATE_TEST_SUITE_P(WithoutDescriptors, TProtobufTableIoTest, ::testing::Values(false));
