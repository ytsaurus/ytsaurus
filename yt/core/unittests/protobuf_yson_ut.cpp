#include <yt/core/test_framework/framework.h>

#include <yt/core/unittests/proto/protobuf_yson_ut.pb.h>
#include <yt/core/unittests/proto/protobuf_yson_casing_ut.pb.h>

#include <yt/core/yson/protobuf_interop.h>
#include <yt/core/yson/null_consumer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/misc/string.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

#include <contrib/libs/protobuf/wire_format.h>

namespace NYT {
namespace NYson {
namespace {

using namespace NYTree;
using namespace NYPath;
using namespace ::google::protobuf::io;
using namespace ::google::protobuf::internal;

////////////////////////////////////////////////////////////////////////////////

TString ToHex(const TString& data)
{
    TStringBuilder builder;
    for (char ch : data) {
        builder.AppendFormat("%x%x ",
            static_cast<unsigned char>(ch) >> 4,
            static_cast<unsigned char>(ch) & 0xf);
    }
    return builder.Flush();
}

#define EXPECT_YPATH(body, ypath) \
    do { \
        bool thrown = false; \
        try body \
        catch (const TErrorException& ex) { \
            thrown = true; \
            Cerr << ToString(ex.Error()) << Endl; \
            EXPECT_EQ(ypath, ex.Error().Attributes().Get<TYPath>("ypath")); \
        } \
        EXPECT_TRUE(thrown); \
    } while (false);

#define TEST_PROLOGUE_WITH_OPTIONS(type, options) \
    TString str; \
    StringOutputStream output(&str); \
    auto protobufWriter = CreateProtobufWriter(&output, ReflectProtobufMessageType<NYT::NProto::type>(), options); \
    BuildYsonFluently(protobufWriter.get())

#define TEST_PROLOGUE(type) \
    TEST_PROLOGUE_WITH_OPTIONS(type, TProtobufWriterOptions())

#define TEST_EPILOGUE(type) \
    Cerr << ToHex(str) << Endl; \
    NYT::NProto::type message; \
    EXPECT_TRUE(message.ParseFromArray(str.data(), str.length()));

TEST(TYsonToProtobufYsonTest, Success)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field").Value(10000)
            .Item("uint32_field").Value(10000U)
            .Item("sint32_field").Value(10000)
            .Item("int64_field").Value(10000)
            .Item("uint64_field").Value(10000U)
            .Item("fixed32_field").Value(10000U)
            .Item("fixed64_field").Value(10000U)
            .Item("bool_field").Value(true)
            .Item("repeated_int32_field").BeginList()
                .Item().Value(1)
                .Item().Value(2)
                .Item().Value(3)
            .EndList()
            .Item("nested_message1").BeginMap()
                .Item("int32_field").Value(123)
                .Item("color").Value("blue")
                .Item("nested_message").BeginMap()
                    .Item("color").Value("green")
                    .Item("nested_message").BeginMap()
                    .EndMap()
                .EndMap()
            .EndMap()
            .Item("nested_message2").BeginMap()
            .EndMap()
            .Item("string_field").Value("hello")
            .Item("repeated_nested_message").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("float_field").Value(3.14)
            .Item("double_field").Value(3.14)
            .Item("attributes").BeginMap()
                .Item("k1").Value(1)
                .Item("k2").Value("test")
                .Item("k3").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("yson_field").BeginMap()
                .Item("a").Value(1)
                .Item("b").BeginList()
                    .Item().Value("foobar")
                .EndList()
            .EndMap()
        .EndMap();


    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(10000, message.int32_field_xxx());
    EXPECT_EQ(10000U, message.uint32_field());
    EXPECT_EQ(10000, message.sint32_field());
    EXPECT_EQ(10000, message.int64_field());
    EXPECT_EQ(10000U, message.uint64_field());
    EXPECT_EQ(10000U, message.fixed32_field());
    EXPECT_EQ(10000U, message.fixed64_field());
    EXPECT_TRUE(message.bool_field());
    EXPECT_EQ("hello", message.string_field());
    EXPECT_FLOAT_EQ(3.14, message.float_field());
    EXPECT_DOUBLE_EQ(3.14, message.double_field());

    EXPECT_TRUE(message.has_nested_message1());
    EXPECT_EQ(123, message.nested_message1().int32_field());
    EXPECT_EQ(NYT::NProto::EColor::Color_Blue, message.nested_message1().color());
    EXPECT_TRUE(message.nested_message1().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().has_int32_field());
    EXPECT_EQ(NYT::NProto::EColor::Color_Green, message.nested_message1().nested_message().color());
    EXPECT_TRUE(message.nested_message1().nested_message().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().nested_message().has_nested_message());
    EXPECT_FALSE(message.nested_message1().nested_message().nested_message().has_int32_field());

    EXPECT_TRUE(message.has_nested_message2());
    EXPECT_FALSE(message.nested_message2().has_int32_field());
    EXPECT_FALSE(message.nested_message2().has_nested_message());

    EXPECT_EQ(3, message.repeated_int32_field().size());
    EXPECT_EQ(1, message.repeated_int32_field().Get(0));
    EXPECT_EQ(2, message.repeated_int32_field().Get(1));
    EXPECT_EQ(3, message.repeated_int32_field().Get(2));

    EXPECT_EQ(2, message.repeated_nested_message().size());
    EXPECT_EQ(456, message.repeated_nested_message().Get(0).int32_field());
    EXPECT_EQ(654, message.repeated_nested_message().Get(1).int32_field());

    EXPECT_EQ(3, message.attributes().attributes_size());
    EXPECT_EQ("k1", message.attributes().attributes(0).key());
    EXPECT_EQ(ConvertToYsonString(1).GetData(), message.attributes().attributes(0).value());
    EXPECT_EQ("k2", message.attributes().attributes(1).key());
    EXPECT_EQ(ConvertToYsonString("test").GetData(), message.attributes().attributes(1).value());
    EXPECT_EQ("k3", message.attributes().attributes(2).key());
    EXPECT_EQ(ConvertToYsonString(std::vector<int>{1, 2, 3}).GetData(), message.attributes().attributes(2).value());

    auto node = BuildYsonNodeFluently().BeginMap()
            .Item("a").Value(1)
            .Item("b").BeginList()
                .Item().Value("foobar")
            .EndList()
        .EndMap();

    EXPECT_EQ(ConvertToYsonString(node).GetData(), message.yson_field());
}

TEST(TYsonToProtobufTest, TypeConversions)
{
    TEST_PROLOGUE(TMessage)
        .BeginMap()
            .Item("int32_field").Value(10000U)
            .Item("uint32_field").Value(10000)
            .Item("sint32_field").Value(10000U)
            .Item("int64_field").Value(10000U)
            .Item("uint64_field").Value(10000)
            .Item("fixed32_field").Value(10000)
            .Item("fixed64_field").Value(10000)
        .EndMap();

    TEST_EPILOGUE(TMessage)
    EXPECT_EQ(10000, message.int32_field_xxx());
    EXPECT_EQ(10000U, message.uint32_field());
    EXPECT_EQ(10000, message.sint32_field());
    EXPECT_EQ(10000, message.int64_field());
    EXPECT_EQ(10000U, message.uint64_field());
    EXPECT_EQ(10000U, message.fixed32_field());
    EXPECT_EQ(10000U, message.fixed64_field());
}

TEST(TYsonToProtobufTest, Failure)
{
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .Value(0);
    }, "/");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(true)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(static_cast<i64>(std::numeric_limits<i32>::max()) + 1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(static_cast<i64>(std::numeric_limits<i32>::min()) - 1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(static_cast<ui64>(std::numeric_limits<ui32>::max()) + 1)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value("test")
                .EndMap()
            .EndMap();
    }, "/nested_message1/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").BeginAttributes().EndAttributes().Value(123)
                .EndMap()
            .EndMap();
    }, "/nested_message1/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("color").Value("white")
                .EndMap()
            .EndMap();
    }, "/nested_message1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("nested_message1").Value(123)
            .EndMap();
    }, "/nested_message1");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message").BeginList()
                    .Item().BeginMap()
                        .Item("color").Value("blue")
                    .EndMap()
                    .Item().BeginMap()
                        .Item("color").Value("black")
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message/1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message").BeginList()
                    .Item().BeginList()
                    .EndList()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message/0");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message").BeginList()
                    .Item().BeginMap()
                        .Item("color").Value("black")
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message/0/color");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(0)
                .Item("int32_field").Value(1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(0)
                .Item("int32_field").Value(1)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessageWithRequiredFields)
            .BeginMap()
                .Item("required_field").Value(0)
                .Item("required_field").Value(1)
            .EndMap();
    }, "/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessageWithRequiredFields)
            .BeginMap()
            .EndMap();
    }, "/required_field");

    // int32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(10000000000)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(10000000000U)
            .EndMap();
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("int32_field").Value(-10000000000)
            .EndMap();
    }, "/int32_field");

    // sint32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(10000000000)
            .EndMap();
    }, "/sint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(10000000000U)
            .EndMap();
    }, "/sint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("sint32_field").Value(-10000000000)
            .EndMap();
    }, "/sint32_field");

    // uint32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(10000000000)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(10000000000U)
            .EndMap();
    }, "/uint32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("uint32_field").Value(-1)
            .EndMap();
    }, "/uint32_field");

    // int32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("int64_field").Value(std::numeric_limits<ui64>::max())
            .EndMap();
    }, "/int64_field");

    // uint64
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("uint64_field").Value(-1)
            .EndMap();
    }, "/uint64_field");

    // fixed32
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("fixed32_field").Value(10000000000)
            .EndMap();
    }, "/fixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("fixed32_field").Value(10000000000U)
            .EndMap();
    }, "/fixed32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("fixed32_field").Value(-10000000000)
            .EndMap();
    }, "/fixed32_field");

    // fixed64
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                  .Item("fixed64_field").Value(-1)
            .EndMap();
    }, "/fixed64_field");
}

TEST(TYsonToProtobufTest, ErrorProto)
{
    TEST_PROLOGUE(TError)
        .BeginMap()
            .Item("message").Value("Hello world")
            .Item("code").Value(1)
            .Item("attributes").BeginMap()
                .Item("host").Value("localhost")
            .EndMap()
        .EndMap();

    TEST_EPILOGUE(TError);

    EXPECT_EQ("Hello world", message.message());
    EXPECT_EQ(1, message.code());

    auto attribute = message.attributes().attributes()[0];
    EXPECT_EQ(attribute.key(), "host");
    EXPECT_EQ(ConvertTo<TString>(TYsonString(attribute.value())), "localhost");
}

TEST(TYsonToProtobufTest, UnknownFields)
{
    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("unknown_field").Value(1)
            .EndMap();
    }, "/");

    EXPECT_YPATH({
        TEST_PROLOGUE(TMessage)
            .BeginMap()
                .Item("repeated_nested_message").BeginList()
                    .Item().BeginMap()
                        .Item("unknown_field").Value(1)
                    .EndMap()
                .EndList()
            .EndMap();
    }, "/repeated_nested_message/0");

    {
        TProtobufWriterOptions options;
        options.SkipUnknownFields = true;

        TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
            .BeginMap()
                .Item("int32_field").Value(10000)
                .Item("unknown_field").Value(1)
                .Item("nested_message1").BeginMap()
                    .Item("int32_field").Value(123)
                    .Item("nested_message").BeginMap()
                        .Item("unknown_map").BeginMap()
                        .EndMap()
                    .EndMap()
                .EndMap()
                .Item("repeated_nested_message").BeginList()
                    .Item().BeginMap()
                        .Item("int32_field").Value(456)
                        .Item("unknown_list").BeginList()
                        .EndList()
                    .EndMap()
                .EndList()
            .EndMap();

        TEST_EPILOGUE(TMessage)
        EXPECT_EQ(10000, message.int32_field_xxx());

        EXPECT_TRUE(message.has_nested_message1());
        EXPECT_EQ(123, message.nested_message1().int32_field());
        EXPECT_TRUE(message.nested_message1().has_nested_message());

        EXPECT_EQ(1, message.repeated_nested_message().size());
        EXPECT_EQ(456, message.repeated_nested_message().Get(0).int32_field());
    }
}

TEST(TYsonToProtobufTest, Entities)
{
    TProtobufWriterOptions options;

    TEST_PROLOGUE_WITH_OPTIONS(TMessage, options)
        .BeginMap()
            .Item("nested_message1").Entity()
        .EndMap();
    TEST_EPILOGUE(TMessage)

    EXPECT_FALSE(message.has_nested_message1());
}

#undef TEST_PROLOGUE
#undef TEST_PROLOGUE_WITH_OPTIONS
#undef TEST_EPILOGUE

////////////////////////////////////////////////////////////////////////////////

#define TEST_PROLOGUE() \
    TString protobuf; \
    StringOutputStream protobufOutputStream(&protobuf); \
    CodedOutputStream codedStream(&protobufOutputStream);

#define TEST_EPILOGUE_WITH_OPTIONS(type, options) \
    codedStream.Trim(); \
    Cerr << ToHex(protobuf) << Endl; \
    ArrayInputStream protobufInputStream(protobuf.data(), protobuf.length()); \
    TString yson; \
    TStringOutput ysonOutputStream(yson); \
    TYsonWriter writer(&ysonOutputStream, EYsonFormat::Pretty); \
    ParseProtobuf(&writer, &protobufInputStream, ReflectProtobufMessageType<NYT::NProto::type>(), options); \
    Cerr << ConvertToYsonString(TYsonString(yson), EYsonFormat::Pretty).GetData() << Endl;

#define TEST_EPILOGUE(type) \
    TEST_EPILOGUE_WITH_OPTIONS(type, TProtobufParserOptions())

TEST(TProtobufToYsonTest, Success)
{
    NYT::NProto::TMessage message;
    message.set_int32_field_xxx(10000);
    message.set_uint32_field(10000U);
    message.set_sint32_field(10000);
    message.set_int64_field(10000);
    message.set_uint64_field(10000U);
    message.set_fixed32_field(10000U);
    message.set_fixed64_field(10000U);
    message.set_bool_field(true);
    message.set_string_field("hello");
    message.set_float_field(3.14);
    message.set_double_field(3.14);

    message.add_repeated_int32_field(1);
    message.add_repeated_int32_field(2);
    message.add_repeated_int32_field(3);

    message.mutable_nested_message1()->set_int32_field(123);
    message.mutable_nested_message1()->set_color(NYT::NProto::Color_Blue);

    message.mutable_nested_message1()->mutable_nested_message()->set_color(NYT::NProto::Color_Green);

    {
        auto* proto = message.add_repeated_nested_message();
        proto->set_int32_field(456);
        proto->add_repeated_int32_field(1);
        proto->add_repeated_int32_field(2);
        proto->add_repeated_int32_field(3);
    }
    {
        auto* proto = message.add_repeated_nested_message();
        proto->set_int32_field(654);
    }
    {
        auto* proto = message.mutable_attributes();
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k1");
            entry->set_value(ConvertToYsonString(1).GetData());
        }
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k2");
            entry->set_value(ConvertToYsonString("test").GetData());
        }
        {
            auto* entry = proto->add_attributes();
            entry->set_key("k3");
            entry->set_value(ConvertToYsonString(std::vector<int>{1, 2, 3}).GetData());
        }
    }

    message.set_yson_field("{a=1;b=[\"foobar\";];}");

    TEST_PROLOGUE()
    message.SerializeToCodedStream(&codedStream);
    TEST_EPILOGUE(TMessage)

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_field").Value(10000)
            .Item("uint32_field").Value(10000U)
            .Item("sint32_field").Value(10000)
            .Item("int64_field").Value(10000)
            .Item("uint64_field").Value(10000U)
            .Item("fixed32_field").Value(10000U)
            .Item("fixed64_field").Value(10000U)
            .Item("bool_field").Value(true)
            .Item("string_field").Value("hello")
            .Item("float_field").Value(3.14)
            .Item("double_field").Value(3.14)
            .Item("repeated_int32_field").BeginList()
                .Item().Value(1)
                .Item().Value(2)
                .Item().Value(3)
            .EndList()
            .Item("nested_message1").BeginMap()
                .Item("int32_field").Value(123)
                .Item("color").Value("blue")
                .Item("nested_message").BeginMap()
                    .Item("color").Value("green")
                .EndMap()
            .EndMap()
            .Item("repeated_nested_message").BeginList()
                .Item().BeginMap()
                    .Item("int32_field").Value(456)
                    .Item("repeated_int32_field").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                        .Item().Value(3)
                    .EndList()
                .EndMap()
                .Item().BeginMap()
                    .Item("int32_field").Value(654)
                .EndMap()
            .EndList()
            .Item("attributes").BeginMap()
                .Item("k1").Value(1)
                .Item("k2").Value("test")
                .Item("k3").BeginList()
                    .Item().Value(1)
                    .Item().Value(2)
                    .Item().Value(3)
                .EndList()
            .EndMap()
            .Item("yson_field").BeginMap()
                .Item("a").Value(1)
                .Item("b").BeginList()
                    .Item().Value("foobar")
                .EndList()
            .EndMap()
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, Casing)
{
    NYT::NProto::TCamelCaseStyleMessage message;
    message.set_somefield(1);
    message.set_anotherfield123(2);
    message.set_crazy_field(3);

    TEST_PROLOGUE()
    message.SerializeToCodedStream(&codedStream);
    TEST_EPILOGUE(TCamelCaseStyleMessage)

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("some_field").Value(1)
            .Item("another_field123").Value(2)
            .Item("crazy_field").Value(3)
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, ErrorProto)
{
    NYT::NProto::TError errorProto;
    errorProto.set_message("Hello world");
    errorProto.set_code(1);
    auto attributeProto = errorProto.mutable_attributes()->add_attributes();
    attributeProto->set_key("host");
    attributeProto->set_value(ConvertToYsonString("localhost").GetData());

    auto serialized = SerializeProtoToRef(errorProto);

    ArrayInputStream inputStream(serialized.Begin(), serialized.Size());
    TString yson;
    TStringOutput outputStream(yson);
    TYsonWriter writer(&outputStream, EYsonFormat::Pretty);
    ParseProtobuf(&writer, &inputStream, ReflectProtobufMessageType<NYT::NProto::TError>());

    auto writtenNode = ConvertToNode(TYsonString(yson));
    auto expectedNode = BuildYsonNodeFluently()
        .BeginMap()
            .Item("message").Value("Hello world")
            .Item("code").Value(1)
            .Item("attributes").BeginMap()
                .Item("host").Value("localhost")
            .EndMap()
        .EndMap();
    EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
}

TEST(TProtobufToYsonTest, Failure)
{
    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(15 /*nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(100);
        TEST_EPILOGUE(TMessage)
    }, "/nested_message1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_int32_field/0");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(1);
        codedStream.WriteTag(WireFormatLite::MakeTag(17 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_int32_field/1");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(4);
        TEST_EPILOGUE(TMessage)
    }, "/repeated_nested_message/1/color");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(18 /*repeated_nested_message*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(6);
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*repeated_int32_field*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        TEST_EPILOGUE(TMessage)
    }, "/repeated_nested_message/1/repeated_int32_field/1");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(3 /*nested_messages*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/nested_messages/0/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(3 /*nested_messages*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*required_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*required_field*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessageWithRequiredFields)
    }, "/nested_messages/0/required_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*int32_field_xxx*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/int32_field");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(2);
        codedStream.WriteTag(WireFormatLite::MakeTag(2 /*value*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");

    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*attributes*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(6);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*attribute*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(4);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        codedStream.WriteTag(WireFormatLite::MakeTag(1 /*key*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(0);
        TEST_EPILOGUE(TMessage)
    }, "/attributes");
}

TEST(TProtobufToYsonTest, UnknownFields)
{
    EXPECT_YPATH({
        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*unknown*/, WireFormatLite::WIRETYPE_FIXED32));
        TEST_EPILOGUE(TMessage)
    }, "/");

    {
        TProtobufParserOptions options;
        options.SkipUnknownFields = true;

        TEST_PROLOGUE()
        codedStream.WriteTag(WireFormatLite::MakeTag(100 /*unknown*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(9);
        codedStream.WriteRaw("blablabla", 9);
        codedStream.WriteTag(WireFormatLite::MakeTag(15 /*nested_message1*/, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
        codedStream.WriteVarint64(3);
        codedStream.WriteTag(WireFormatLite::MakeTag(19 /*color*/, WireFormatLite::WIRETYPE_VARINT));
        codedStream.WriteVarint64(2 /*red*/);
        TEST_EPILOGUE_WITH_OPTIONS(TMessage, options)

        auto writtenNode = ConvertToNode(TYsonString(yson));
        auto expectedNode = BuildYsonNodeFluently()
            .BeginMap()
                .Item("nested_message1").BeginMap()
                    .Item("color").Value("red")
                .EndMap()
            .EndMap();
        EXPECT_TRUE(AreNodesEqual(writtenNode, expectedNode));
    }
}

#undef TEST_PROLOGUE
#undef TEST_EPILOGUE
#undef TEST_EPILOGUE_WITH_OPTIONS

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYson
} // namespace NYT
