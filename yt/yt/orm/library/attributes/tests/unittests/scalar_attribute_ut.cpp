#include <yt/yt/orm/library/attributes/tests/proto/scalar_attribute.pb.h>

#include <yt/yt/orm/library/attributes/scalar_attribute.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/node.h>

#include <google/protobuf/io/coded_stream.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

template<typename TProtoMessage>
NYTree::INodePtr MessageToNode(const TProtoMessage& message)
{
    TString newYsonString;
    TStringOutput newYsonOutputStream(newYsonString);
    NYson::TYsonWriter ysonWriter(&newYsonOutputStream, NYson::EYsonFormat::Pretty);
    TString protobufString = message.SerializeAsString();
    google::protobuf::io::ArrayInputStream protobufInput(protobufString.data(), protobufString.length());
    NYson::ParseProtobuf(&ysonWriter, &protobufInput, NYson::ReflectProtobufMessageType<TProtoMessage>());
    return NYTree::ConvertToNode(NYson::TYsonString(newYsonString));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClearAttributesTest, EmptyPath)
{
    NProto::TMessage message;
    message.set_int64_field(1);
    ClearProtobufFieldByPath(message, "", false);
    // Empty message has zero byte size.
    EXPECT_EQ(0u, message.ByteSizeLong());
}

TEST(TClearAttributesTest, Asterisk)
{
    NProto::TMessage message;
    message.set_int64_field(1);
    ClearProtobufFieldByPath(message, "/*", false);
    // Empty message has zero byte size.
    EXPECT_EQ(0u, message.ByteSizeLong());

    // This is legal. Imagine all fields in a message have the same shape. Not in our case though.
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/*/nested_message", false),
        TErrorException);
    // This is not a missing entry, this is supplying a string key to a repeated field.
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/*/nested_message", true),
        TErrorException);
}

TEST(TClearAttributesTest, SimpleField)
{
    NProto::TMessage message;
    message.set_int64_field(1);
    message.mutable_nested_message()->set_int32_field(2);
    message.mutable_nested_message()->mutable_nested_message()->set_int32_field(2);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/int64_field", false));
    EXPECT_FALSE(message.has_int64_field());
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/int64_field", true));
    EXPECT_FALSE(message.has_int64_field());
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/nested_message", false));
    EXPECT_FALSE(message.nested_message().has_nested_message());
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/nested_message", true));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/int64_field", false),
        TErrorException);
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/nested_message/nested_message/int32_field", false),
        TErrorException);

    EXPECT_FALSE(message.has_int64_field());
    EXPECT_FALSE(message.nested_message().has_nested_message());
    EXPECT_EQ(2, message.nested_message().int32_field());
}

TEST(TClearAttributesTest, MapField)
{
    NProto::TMessage message;
    {
        auto& map = *message.mutable_string_to_int32_map();
        map["a"] = 1;
        map["b"] = 2;
    }

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map", false));
    EXPECT_FALSE(message.string_to_int32_map().contains("a"));
    EXPECT_FALSE(message.string_to_int32_map().contains("b"));

    // It's OK to clean an empty map again.
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map", false));
}

TEST(TClearAttributesTest, MapFieldAsterisk)
{
    NProto::TMessage message;
    {
        auto& map = *message.mutable_string_to_int32_map();
        map["a"] = 1;
        map["b"] = 2;
    }

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map/*", false));
    EXPECT_FALSE(message.string_to_int32_map().contains("a"));
    EXPECT_FALSE(message.string_to_int32_map().contains("b"));

    // The message is there, it's OK to clean it again.
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map/*", false));
}

TEST(TClearAttributesTest, MapFieldItem)
{
    NProto::TMessage message;
    {
        auto& map = *message.mutable_string_to_int32_map();
        map["a"] = 1;
        map["b"] = 2;
    }

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map/a", false));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/string_to_int32_map/a", true));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/string_to_int32_map/a", false),
        TErrorException);
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/string_to_int32_map/c", false),
        TErrorException);

    EXPECT_FALSE(message.string_to_int32_map().contains("a"));
    EXPECT_EQ(2, message.string_to_int32_map().at("b"));
}

TEST(TClearAttributesTest, MapItemNestedAsterisk)
{
    NProto::TMessage message;
    {
        auto& map = *message.mutable_nested_message()->mutable_nested_message_map();
        map["a"].mutable_nested_message()->set_int32_field(1);
        map["b"].mutable_nested_message()->set_int32_field(2);
    }

    EXPECT_NO_THROW(
        ClearProtobufFieldByPath(
            message,
            "/nested_message/nested_message_map/*/nested_message/int32_field",
            false));
    EXPECT_FALSE(message.nested_message().nested_message_map().at("a").nested_message().has_int32_field());
    EXPECT_FALSE(message.nested_message().nested_message_map().at("b").nested_message().has_int32_field());

    // We expect these paths to be present in every entry now.
    EXPECT_THROW(
        ClearProtobufFieldByPath(
            message,
            "/nested_message/nested_message_map/*/nested_message/int32_field",
            false),
        TErrorException);
}

TEST(TClearAttributesTest, MapItemNestedField)
{
    NProto::TMessage message;
    {
        auto& map = *message.mutable_nested_message()->mutable_nested_message_map();
        map["a"].mutable_nested_message()->set_int32_field(1);
        map["b"].mutable_nested_message()->set_int32_field(2);
    }

    EXPECT_NO_THROW(
        ClearProtobufFieldByPath(
            message,
            "/nested_message/nested_message_map/a/nested_message/int32_field",
            false));
    EXPECT_FALSE(message.nested_message().nested_message_map().at("a").nested_message().has_int32_field());
    EXPECT_EQ(0, message.nested_message().nested_message_map().at("a").nested_message().int32_field());

    EXPECT_THROW(
        ClearProtobufFieldByPath(
            message,
            "/nested_message/nested_message_map/c/nested_message/int32_field",
            false),
        TErrorException);
    EXPECT_NO_THROW(
        ClearProtobufFieldByPath(
            message,
            "/nested_message/nested_message_map/c/nested_message/int32_field",
            true));

    EXPECT_EQ(2, message.nested_message().nested_message_map().at("b").nested_message().int32_field());
}

TEST(TClearAttributesTest, ListField)
{
    NProto::TMessage message;
    message.add_repeated_int32_field(1);
    message.add_repeated_int32_field(2);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_int32_field", false));
    ASSERT_EQ(0, message.repeated_int32_field().size());

    // Repeated fields may be empty.
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_int32_field", false));
}

TEST(TClearAttributesTest, ListFieldAsterisk)
{
    NProto::TMessage message;
    message.add_repeated_int32_field(1);
    message.add_repeated_int32_field(2);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_int32_field/*", false));
    ASSERT_EQ(0, message.repeated_int32_field().size());

    // Repeated fields may be empty.
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_int32_field/*", false));
}

TEST(TClearAttributesTest, ListItem)
{
    NProto::TMessage message;
    message.add_repeated_int32_field(1);
    message.add_repeated_int32_field(2);
    auto* nested = message.add_repeated_nested_message();
    nested->set_int32_field(3);
    nested->add_repeated_int32_field(5);
    message.add_repeated_nested_message()->set_int32_field(4);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_int32_field/0", false));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/repeated_int32_field/1", false),
        TErrorException);
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_nested_message/1", false));
    EXPECT_NO_THROW(
        ClearProtobufFieldByPath(
            message,
            "/repeated_nested_message/0/repeated_int32_field/0",
            false));

    ASSERT_EQ(1, message.repeated_int32_field().size());
    EXPECT_EQ(2, message.repeated_int32_field().at(0));
    ASSERT_EQ(1, message.repeated_nested_message().size());
    EXPECT_EQ(3, message.repeated_nested_message().at(0).int32_field());
    EXPECT_EQ(0, message.repeated_nested_message().at(0).repeated_int32_field().size());
}

TEST(TClearAttributesTest, ListItemNestedField)
{
    NProto::TMessage message;
    message.add_repeated_nested_message()->set_int32_field(1);
    message.add_repeated_nested_message()->set_int32_field(2);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_nested_message/1/int32_field", false));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_nested_message/1/int32_field", true));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/repeated_nested_message/2/int32_field", false),
        TErrorException);

    ASSERT_EQ(2, message.repeated_nested_message().size());
    EXPECT_EQ(1, message.repeated_nested_message().at(0).int32_field());
    EXPECT_FALSE(message.repeated_nested_message().at(1).has_int32_field());
}

TEST(TClearAttributesTest, ListItemNestedFieldAsterisk)
{
    NProto::TMessage message;
    message.add_repeated_nested_message()->set_int32_field(1);
    message.add_repeated_nested_message()->set_int32_field(2);

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/repeated_nested_message/*/int32_field", false));
    ASSERT_EQ(2, message.repeated_nested_message().size());
    EXPECT_FALSE(message.repeated_nested_message().at(0).has_int32_field());
    EXPECT_FALSE(message.repeated_nested_message().at(1).has_int32_field());

    // But now we expect to have this field in every message.
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/repeated_nested_message/*/int32_field", false),
        TErrorException);
    EXPECT_NO_THROW(
        ClearProtobufFieldByPath(message, "/repeated_nested_message/*/int32_field", true));
}

TEST(TClearAttributesTest, UnknownYsonField)
{
    auto ysonString = NYTree::BuildYsonStringFluently()
        .BeginMap()
            .Item("int64_field").Value(1)
            .Item("unknown_int1").Value(2)
            .Item("unknown_int2").Value(3)
            .Item("nested_message").BeginMap()
                .Item("int32_field").Value(4)
                .Item("unknown_string").Value("a")
            .EndMap()
        .EndMap();

    TString protobufString;
    google::protobuf::io::StringOutputStream protobufOutput(&protobufString);
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
        NYson::EUnknownYsonFieldsMode::Keep);
    auto protobufWriter = NYson::CreateProtobufWriter(
        &protobufOutput, NYson::ReflectProtobufMessageType<NProto::TMessage>(), options);
    NYson::ParseYsonStringBuffer(ysonString.ToString(), NYson::EYsonType::Node, protobufWriter.get());

    NProto::TMessage message;
    EXPECT_TRUE(message.ParseFromArray(protobufString.data(), protobufString.length()));

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/unknown_int1", false));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_string", false));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/unknown_int1", true));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_string", true));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/unknown_string", true));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/unknown_string", false),
        TErrorException);

    EXPECT_EQ(1, message.int64_field());
    EXPECT_EQ(4, message.nested_message().int32_field());

    auto node = MessageToNode(message);
    EXPECT_FALSE(node->AsMap()->FindChild("unknown_int1"));
    auto int2 = node->AsMap()->FindChild("unknown_int2");
    ASSERT_TRUE(int2);
    EXPECT_EQ(3, int2->AsInt64()->GetValue());
    auto nested = node->AsMap()->FindChild("nested_message");
    ASSERT_TRUE(nested);
    EXPECT_FALSE(nested->AsMap()->FindChild("unknown_string"));
}

TEST(TClearAttributesTest, UnknownYsonNestedField)
{
    auto ysonString = NYTree::BuildYsonStringFluently()
        .BeginMap()
            .Item("nested_message").BeginMap()
                .Item("unknown_map").BeginMap()
                    .Item("key1").Value(1)
                    .Item("key2").Value(2)
                    .Item("key3").BeginList()
                        .Item().Value(1)
                        .Item().Value(2)
                    .EndList()
                .EndMap()
            .EndMap()
        .EndMap();

    TString protobufString;
    google::protobuf::io::StringOutputStream protobufOutput(&protobufString);
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
        NYson::EUnknownYsonFieldsMode::Keep);
    auto protobufWriter = NYson::CreateProtobufWriter(
        &protobufOutput, NYson::ReflectProtobufMessageType<NProto::TMessage>(), options);
    NYson::ParseYsonStringBuffer(ysonString.ToString(), NYson::EYsonType::Node, protobufWriter.get());

    NProto::TMessage message;
    EXPECT_TRUE(message.ParseFromArray(protobufString.data(), protobufString.length()));

    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_map/key1", false));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_map/key3/0", false));
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/nested_message/unknown_map/key1", false),
        TErrorException);
    EXPECT_THROW(
        ClearProtobufFieldByPath(message, "/nested_message/unknown_map1/key1", false),
        TErrorException);
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_map/key1", true));
    EXPECT_NO_THROW(ClearProtobufFieldByPath(message, "/nested_message/unknown_map1/key1", true));

    auto root = MessageToNode(message);
    auto nested = root->AsMap()->FindChild("nested_message");
    ASSERT_TRUE(nested);
    auto map = nested->AsMap()->FindChild("unknown_map");
    ASSERT_FALSE(map->AsMap()->FindChild("key1"));
    ASSERT_TRUE(map->AsMap()->FindChild("key2"));
    auto list = map->AsMap()->FindChild("key3");
    ASSERT_TRUE(list);
    ASSERT_EQ(1, list->AsList()->GetChildCount());
    ASSERT_EQ(2, list->AsList()->FindChild(0)->AsInt64()->GetValue());
}

////////////////////////////////////////////////////////////////////////////////

class TSetAttributeTest
    : public testing::TestWithParam<bool>
{
public:
    // NB! Processing values related to the root of protobuf maps is unimplemented.
    // Tests on maps should not rely on yson to wire string conversion.
    // TODO(grigminakov): Add support for maps and then move this helper function to common library.
    void BuildWireStringFromNodePtr(
        std::vector<std::string>& wireStringBuffer,
        const NYTree::INodePtr& value,
        const NYson::TProtobufElement& element,
        const NYson::TProtobufWriterOptions& options)
    {
        auto asScalar = [] (const NYson::TProtobufElement& element) -> NYson::TProtobufScalarElement {
            auto* scalarElement = std::get_if<std::unique_ptr<NYson::TProtobufScalarElement>>(&element);
            THROW_ERROR_EXCEPTION_UNLESS(scalarElement,
                "Expected scalar protobuf element");
            return *scalarElement->get();
        };

        switch (value->GetType()) {
            case NYTree::ENodeType::Map: {
                if (auto* messageElement = std::get_if<std::unique_ptr<NYson::TProtobufMessageElement>>(&element)) {
                    wireStringBuffer.push_back(SerializeMessage(value, messageElement->get()->Type, options));
                } else if (std::holds_alternative<std::unique_ptr<NYson::TProtobufAttributeDictionaryElement>>(element)) {
                    EXPECT_EQ(value->GetType(), NYTree::ENodeType::Map);
                    wireStringBuffer.push_back(SerializeAttributeDictionary(
                        *NYTree::IAttributeDictionary::FromMap(value->AsMap())));
                } else {
                    THROW_ERROR_EXCEPTION(
                        "Encountered unexpected protobuf element in yson-map to wire string conversion");
                }
                break;
            }
            case NYTree::ENodeType::List: {
                auto* repeatedElement = std::get_if<std::unique_ptr<NYson::TProtobufRepeatedElement>>(&element);
                THROW_ERROR_EXCEPTION_UNLESS(repeatedElement,
                    "Expected repeated protobuf element");
                for (const auto& [index, child] : SEnumerate(value->AsList()->GetChildren())) {
                    BuildWireStringFromNodePtr(
                        wireStringBuffer,
                        child,
                        std::get<std::unique_ptr<NYson::TProtobufRepeatedElement>>(element)->Element,
                        options.CreateChildOptions(ToString(index)));
                }
                break;
            }
            case NYTree::ENodeType::Entity: {
                // NB! Empty wire string represents entity.
                break;
            }
            case NYTree::ENodeType::Uint64: {
                wireStringBuffer.push_back(SerializeUint64(value->AsUint64()->GetValue(), asScalar(element).Type));
                break;
            }
            case NYTree::ENodeType::Int64: {
                wireStringBuffer.push_back(SerializeInt64(value->AsInt64()->GetValue(), asScalar(element).Type));
                break;
            }
            case NYTree::ENodeType::Double: {
                wireStringBuffer.push_back(SerializeDouble(value->AsDouble()->GetValue(), asScalar(element).Type));
                break;
            }
            case NYTree::ENodeType::Boolean: {
                wireStringBuffer.push_back(SerializeBoolean(value->AsBoolean()->GetValue(), asScalar(element).Type));
                break;
            }
            case NYTree::ENodeType::String: {
                const auto& stringValue = value->AsString()->GetValue();
                if (asScalar(element).Type.Underlying() == google::protobuf::FieldDescriptor::TYPE_ENUM) {
                    auto encodedEnum = NYson::FindProtobufEnumValueByLiteral<i32>(
                        asScalar(element).EnumType,
                        stringValue);
                    THROW_ERROR_EXCEPTION_UNLESS(encodedEnum.has_value(),
                        "Literal %Qv is not a valid enum value",
                        value->AsString()->GetValue());
                    wireStringBuffer.push_back(SerializeInt64(
                        *encodedEnum,
                        NYson::TProtobufElementType{google::protobuf::FieldDescriptor::TYPE_ENUM}));
                } else {
                    wireStringBuffer.push_back(stringValue);
                }
                break;
            }
            case NYTree::ENodeType::Composite: {
                YT_ABORT();
            }
        }
    }

    void SetProtobufFieldByPath(
        NProtoBuf::Message& message,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& value,
        const NYson::TProtobufWriterOptions& options = {},
        bool recursive = false)
    {
        if (/*setViaYson*/ GetParam()) {
            NAttributes::SetProtobufFieldByPath(message, path, value, options, recursive);
            return;
        }

        auto rootType = NYson::ReflectProtobufMessageType(message.GetDescriptor());
        auto element = NYson::ResolveProtobufElementByYPath(rootType, path, {.AllowUnknownYsonFields = true});

        if (std::holds_alternative<std::unique_ptr<NYson::TProtobufAnyElement>>(element.Element)) {
            NAttributes::SetProtobufFieldByPath(message, path, value, options, recursive);
            return;
        }

        std::vector<std::string> wireStringBuffer;
        BuildWireStringFromNodePtr(wireStringBuffer, value, element.Element, options.CreateChildOptions(path));
        NAttributes::SetProtobufFieldByPath(message, path, TWireString::FromSerialized(wireStringBuffer), recursive);
    }
};

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    TSetAttributeTest,
    TSetAttributeTest,
    /*setViaYson*/ testing::Values(false, true),
    /*evalGenerateName*/ [] (const testing::TestParamInfo<TSetAttributeTest::ParamType>& setViaYson) {
        return setViaYson.param ? "Yson" : "WireString";
    });

////////////////////////////////////////////////////////////////////////////////

TEST_P(TSetAttributeTest, EmptyPath)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("int64_field").Value(1)
        .EndMap();
    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "", node));
    EXPECT_EQ(1, message.int64_field());
}

TEST_P(TSetAttributeTest, Scalar)
{
#define TESTCASE(path, value)                                                           \
    do {                                                                                \
        NProto::TMessage message;                                                       \
        EXPECT_NO_THROW(                                                                \
            SetProtobufFieldByPath(message, "/" #path, NYTree::ConvertToNode(value)));  \
        EXPECT_EQ(value, message.path());                                               \
    } while (false)

    TESTCASE(uint32_field, 1u);
    TESTCASE(int32_field, -1);
    TESTCASE(uint64_field, 1u);
    TESTCASE(sint64_field, -1);
    TESTCASE(fixed32_field, 2u);
    TESTCASE(fixed64_field, 2u);
    TESTCASE(sfixed32_field, -2);
    TESTCASE(sfixed64_field, -2);
    TESTCASE(bool_field, true);
    TESTCASE(string_field, "hello");
    TESTCASE(float_field, 1.0f);
    TESTCASE(double_field, 0.1);
#undef TESTCASE
}

TEST_P(TSetAttributeTest, Message)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("int32_field").Value(4)
        .EndMap();
    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/nested_message", node));
    EXPECT_EQ(4, message.nested_message().int32_field());
}

TEST_P(TSetAttributeTest, NestedMessageField)
{
    auto node = NYTree::ConvertToNode(4);
    NProto::TMessage message;
    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/nested_message/int32_field", node),
        TErrorException);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/nested_message/int32_field", node, {}, true));
    EXPECT_EQ(4, message.nested_message().int32_field());
}

TEST_P(TSetAttributeTest, MapValueNestedField)
{
    auto node = NYTree::ConvertToNode(4);
    NProto::TMessage message;
    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/nested_message_map/a/int32_field", node),
        TErrorException);
    EXPECT_NO_THROW(
        SetProtobufFieldByPath(message, "/nested_message_map/a/int32_field", node, {}, true));
    EXPECT_EQ(4, message.nested_message_map().at("a").int32_field());
    auto node2 = NYTree::ConvertToNode(3);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/nested_message_map/a/int32_field", node2));
    EXPECT_EQ(3, message.nested_message_map().at("a").int32_field());
}

TEST_P(TSetAttributeTest, MapValueScalar)
{
    auto node = NYTree::ConvertToNode(4);
    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map/a", node));
    EXPECT_EQ(4, message.string_to_int32_map().at("a"));
    auto node2 = NYTree::ConvertToNode(3);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map/a", node2));
    EXPECT_EQ(3, message.string_to_int32_map().at("a"));
}

TEST_P(TSetAttributeTest, RepeatedNestedField)
{
    auto node = NYTree::ConvertToNode(4);
    NProto::TMessage message;
    message.add_repeated_nested_message();
    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/repeated_nested_message/1/int32_field", node),
        TErrorException);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_nested_message/0/int32_field", node));
    EXPECT_EQ(4, message.repeated_nested_message().at(0).int32_field());
}

TEST_P(TSetAttributeTest, RepeatedScalar)
{
#define TESTCASE(field, value)                                                                            \
    do {                                                                                                  \
        NProto::TMessage message;                                                                         \
        message.add_ ## field({});                                                                        \
        EXPECT_NO_THROW(                                                                                  \
            SetProtobufFieldByPath(message, "/" #field "/end" , NYTree::ConvertToNode(value)));           \
        EXPECT_EQ(value, message.field().at(1));                                                          \
    } while (false)

    TESTCASE(repeated_uint32_field, 1u);
    TESTCASE(repeated_int32_field, -1);
    TESTCASE(repeated_uint64_field, 1u);
    TESTCASE(repeated_int64_field, -1);
    TESTCASE(repeated_bool_field, true);
    TESTCASE(repeated_float_field, 1.0f);
    TESTCASE(repeated_double_field, 0.1);
#undef TESTCASE
}

TEST(TSetAttributeTest, MapFieldYson)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("a").Value(1)
            .Item("b").Value(2)
        .EndMap();

    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map", node));
    EXPECT_EQ(2u, message.string_to_int32_map().size());
    EXPECT_EQ(1, message.string_to_int32_map().at("a"));
    EXPECT_EQ(2, message.string_to_int32_map().at("b"));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map", NYTree::BuildYsonNodeFluently().Entity()));
    EXPECT_TRUE(message.string_to_int32_map().empty());
}

TEST(TSetAttributeTest, MapFieldWireString)
{
    TString wireStringBuffer;
    {
        NProto::TMessage message;
        message.mutable_string_to_int32_map()->emplace("a", 1);
        message.mutable_string_to_int32_map()->emplace("b", 2);
        wireStringBuffer = message.SerializeAsString();
    }
    auto wireString = GetWireStringByPath(
        NProto::TMessage::descriptor(),
        TWireString::FromSerialized(wireStringBuffer), "/string_to_int32_map");

    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map", wireString));
    EXPECT_EQ(2u, message.string_to_int32_map().size());
    EXPECT_EQ(1, message.string_to_int32_map().at("a"));
    EXPECT_EQ(2, message.string_to_int32_map().at("b"));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/string_to_int32_map", NYTree::BuildYsonNodeFluently().Entity()));
    EXPECT_TRUE(message.string_to_int32_map().empty());
}

TEST_P(TSetAttributeTest, AttributeDictionaryField)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("a").Value("foo_a")
            .Item("b").Value("foo_b")
        .EndMap();

    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/attribute_dictionary", node));
    ASSERT_EQ(2, message.attribute_dictionary().attributes_size());
    EXPECT_EQ("a", message.attribute_dictionary().attributes(0).key());
    EXPECT_EQ(
        "foo_a",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.attribute_dictionary().attributes(0).value())));
    ASSERT_EQ(2, message.attribute_dictionary().attributes_size());
    EXPECT_EQ("b", message.attribute_dictionary().attributes(1).key());
    EXPECT_EQ(
        "foo_b",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.attribute_dictionary().attributes(1).value())));

    EXPECT_NO_THROW(
        SetProtobufFieldByPath(
            message,
            "/attribute_dictionary/aa",
            NYTree::ConvertToNode("foo_aa"),
            /*options*/ {},
            /*recursive*/ true));
    EXPECT_EQ(3, message.attribute_dictionary().attributes_size());
    EXPECT_EQ("aa", message.attribute_dictionary().attributes(1).key());
    EXPECT_EQ(
        "foo_aa",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.attribute_dictionary().attributes(1).value())));
    EXPECT_EQ("b", message.attribute_dictionary().attributes(2).key());
    EXPECT_EQ(
        "foo_b",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.attribute_dictionary().attributes(2).value())));
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/attribute_dictionary",
            NYTree::BuildYsonNodeFluently().Entity()));
    EXPECT_EQ(0, message.attribute_dictionary().attributes_size());
}

TEST_P(TSetAttributeTest, NestedAttributeDictionaryField)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("a").Value("foo_a")
            .Item("b").Value("foo_b")
        .EndMap();

    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(
        message,
        "/nested_message/attribute_dictionary",
        node,
        /*options*/ {},
        /*recursive*/ true));
    ASSERT_EQ(2, message.nested_message().attribute_dictionary().attributes_size());
    EXPECT_EQ("a", message.nested_message().attribute_dictionary().attributes(0).key());
    EXPECT_EQ(
        "foo_a",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.nested_message().attribute_dictionary().attributes(0).value())));
    EXPECT_EQ("b", message.nested_message().attribute_dictionary().attributes(1).key());
    EXPECT_EQ(
        "foo_b",
        NYson::ConvertFromYsonString<TString>(
            NYson::TYsonString(message.nested_message().attribute_dictionary().attributes(1).value())));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/nested_message/attribute_dictionary",
            NYTree::BuildYsonNodeFluently().Entity()));
    EXPECT_EQ(0, message.nested_message().attribute_dictionary().attributes_size());
}

TEST_P(TSetAttributeTest, RepeatedField)
{
#define TESTCASE(field, value1, value2)                                                                   \
    do {                                                                                                  \
        auto node = NYTree::BuildYsonNodeFluently()                                                       \
        .BeginList()                                                                                      \
            .Item().Value(value1)                                                                         \
            .Item().Value(value2)                                                                         \
        .EndList();                                                                                       \
        NProto::TMessage message;                                                                         \
        message.add_ ## field({});                                                                        \
        EXPECT_NO_THROW(                                                                                  \
            SetProtobufFieldByPath(message, "/" #field, node));                                           \
        ASSERT_THAT(message.field(), testing::ElementsAreArray({value1, value2}));                        \
        SetProtobufFieldByPath(message, "/" #field, NYTree::BuildYsonNodeFluently().Entity());            \
        EXPECT_TRUE(message.field().empty());                                                             \
    } while (false)

    TESTCASE(repeated_uint32_field, 1u, 2u);
    TESTCASE(repeated_int32_field, -1, -2);
    TESTCASE(repeated_uint64_field, 1u, 2u);
    TESTCASE(repeated_int64_field, -1, -2);
    TESTCASE(repeated_bool_field, true, false);
    TESTCASE(repeated_float_field, 1.0f, 2.0f);
    TESTCASE(repeated_double_field, 0.1, 0.2);
#undef TESTCASE
}

TEST_P(TSetAttributeTest, ListModification)
{
    NProto::TMessage message;
#define SET(field, ...)                                                                                   \
    do {                                                                                                  \
        auto list = NYTree::BuildYsonNodeFluently().BeginList();                                          \
        for (auto item : __VA_ARGS__) {                                                                   \
            list.Item().Value(item);                                                                      \
        }                                                                                                 \
        auto node = list.EndList();                                                                       \
        ASSERT_NO_THROW(SetProtobufFieldByPath(message, "/" #field, node));                               \
        ASSERT_THAT(message.field(), testing::ElementsAreArray(__VA_ARGS__));                             \
    } while (false)

#define TESTCASE(field, subpath, value, ...)                                                              \
    do {                                                                                                  \
        auto node = NYTree::ConvertToNode(value);                                                         \
        EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/" #field subpath, node));                       \
        EXPECT_THAT(message.field(), testing::ElementsAreArray(__VA_ARGS__));                             \
    } while (false)

    SET(repeated_uint64_field, {1u, 2u, 3u});
    TESTCASE(repeated_uint64_field, "/0", 5u, {5u, 2u, 3u});
    TESTCASE(repeated_uint64_field, "/after:0", 6u, {5u, 6u, 2u, 3u});

    SET(repeated_int64_field, {1, 2, 3});
    TESTCASE(repeated_int64_field, "/-1", 5, {1, 2, 5});
    TESTCASE(repeated_int64_field, "/before:-3", 6, {6, 1, 2, 5});

    SET(repeated_uint32_field, {1u, 2u, 3u});
    TESTCASE(repeated_uint32_field, "/2", 5u, {1u, 2u, 5u});
    TESTCASE(repeated_uint32_field, "/end", 6u, {1u, 2u, 5u, 6u});

    SET(repeated_int32_field, {1, 2, 3});
    TESTCASE(repeated_int32_field, "/-2", 5, {1, 5, 3});
    TESTCASE(repeated_int32_field, "/begin", 6, {6, 1, 5, 3});

    SET(repeated_bool_field, {true, false});
    TESTCASE(repeated_bool_field, "/0", false, {false, false});
    TESTCASE(repeated_bool_field, "/before:0", true, {true, false, false});

    SET(repeated_float_field, {1., 2., 3.});
    TESTCASE(repeated_float_field, "/1", 4., {1., 4., 3.});
    TESTCASE(repeated_float_field, "/before:-1", 5., {1., 4., 5., 3.});

    SET(repeated_double_field, {1., 2., 3.});
    TESTCASE(repeated_double_field, "/1", 4., {1., 4., 3.});
    TESTCASE(repeated_double_field, "/before:3", 5., {1., 4., 3., 5.});

    SET(repeated_string_field, {"a", "b", "c"});
    TESTCASE(repeated_string_field, "/0", "d", {"d", "b", "c"});
    TESTCASE(repeated_string_field, "/after:0", "e", {"d", "e", "b", "c"});

    TESTCASE(repeated_enum_field, "/end", NProto::EColor::C_BLUE, {NProto::EColor::C_BLUE});
    TESTCASE(repeated_enum_field, "/-1", NProto::EColor::C_GREEN, {NProto::EColor::C_GREEN});
    {
        auto node = NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("int32_field").Value(1)
            .EndMap();
        ASSERT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_nested_message/begin", node));
        ASSERT_EQ(message.repeated_nested_message_size(), 1);
        EXPECT_EQ(message.repeated_nested_message(0).int32_field(), 1);
    }
    {
        auto node = NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("int32_field").Value(2)
            .EndMap();
        ASSERT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_nested_message/-1", node));
        ASSERT_EQ(message.repeated_nested_message_size(), 1);
        EXPECT_EQ(message.repeated_nested_message(0).int32_field(), 2);
    }

    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/repeated_uint64_field/100500", NYTree::ConvertToNode(1u)),
        TErrorException);
    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/repeated_uint64_field/before:100500", NYTree::ConvertToNode(1u)),
        TErrorException);

#undef TESTCASE
#undef RESET
}

TEST_P(TSetAttributeTest, RepeatedStringField)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginList()
            .Item().Value("one")
            .Item().Value("two")
        .EndList();

    NProto::TMessage message;
    message.add_repeated_string_field();
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_string_field", node));
    EXPECT_THAT(message.repeated_string_field(), testing::ElementsAreArray({"one", "two"}));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_string_field/end", NYTree::ConvertToNode("three")));
    EXPECT_THAT(message.repeated_string_field(), testing::ElementsAreArray({"one", "two", "three"}));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_string_field/begin", NYTree::ConvertToNode("zero")));
    EXPECT_THAT(message.repeated_string_field(), testing::ElementsAreArray({"zero", "one", "two", "three"}));
}

TEST_P(TSetAttributeTest, EnumField)
{
    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/enum_field", NYTree::ConvertToNode("blue")));
    EXPECT_EQ(NProto::EColor::C_BLUE, message.enum_field());
    EXPECT_THROW_WITH_SUBSTRING(
        SetProtobufFieldByPath(message, "/enum_field", NYTree::ConvertToNode("black")),
        "black");

    EXPECT_NO_THROW(
        SetProtobufFieldByPath(
            message,
            "/enum_field",
            NYTree::ConvertToNode(static_cast<i64>(NProto::EColor::C_GREEN))));
    EXPECT_EQ(NProto::EColor::C_GREEN, message.enum_field());
    EXPECT_THROW_WITH_SUBSTRING(
        SetProtobufFieldByPath(message, "/enum_field", NYTree::ConvertToNode(100500)),
        "100500");
}

TEST_P(TSetAttributeTest, RepeatedEnumField)
{
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginList()
        .Item().Value("green")
        .EndList();
    NProto::TMessage message;
    message.add_repeated_enum_field(NProto::EColor::C_BLUE);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_enum_field", node));
    EXPECT_THAT(
        message.repeated_enum_field(),
        testing::ElementsAreArray({NProto::EColor::C_GREEN}));

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/repeated_enum_field/after:0", NYTree::ConvertToNode("blue")));
    EXPECT_THAT(
        message.repeated_enum_field(),
        testing::ElementsAreArray({NProto::EColor::C_GREEN, NProto::EColor::C_BLUE}));

    EXPECT_THROW_WITH_SUBSTRING(
        SetProtobufFieldByPath(message, "/repeated_enum_field/end", NYTree::ConvertToNode("black")),
        "black");
}

TEST_P(TSetAttributeTest, UnknownYsonFields)
{
    NProto::TMessage message;
    NYson::TProtobufWriterOptions options;

    options.UnknownYsonFieldModeResolver = NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
        NYson::EUnknownYsonFieldsMode::Keep);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/unknown_int1", NYTree::ConvertToNode(2), options));

    options.UnknownYsonFieldModeResolver = NYson::TProtobufWriterOptions::CreateConstantUnknownYsonFieldModeResolver(
        NYson::EUnknownYsonFieldsMode::Skip);
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/unknown_string", NYTree::ConvertToNode("a"), options));

    EXPECT_THROW(
        SetProtobufFieldByPath(message, "/unknown_int2", NYTree::ConvertToNode(3), {}),
        TErrorException);

    auto node = MessageToNode(message);
    auto int1 = node->AsMap()->FindChild("unknown_int1");
    EXPECT_EQ(2, int1->AsInt64()->GetValue());
    EXPECT_FALSE(node->AsMap()->FindChild("unknown_string"));
    EXPECT_FALSE(node->AsMap()->FindChild("unknown_int2"));
}

TEST_P(TSetAttributeTest, UnknownYsonFieldsByPath)
{
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath& path) {
        if (path == "/nested_message/unknown_map") {
            return NYson::EUnknownYsonFieldsMode::Keep;
        }
        return NYson::EUnknownYsonFieldsMode::Fail;
    };
    auto node1 = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("unknown_map")
            .BeginMap()
                .Item("unknown_int").Value(1)
            .EndMap()
        .EndMap();

    auto node2 = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("unknown_int").Value(1)
        .EndMap();

    NProto::TMessage message;
    EXPECT_THROW_WITH_SUBSTRING(
        SetProtobufFieldByPath(message, "/nested_message", node2, options),
        "unknown_int");
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/nested_message", node1, options));

    auto node = MessageToNode(message);
    auto nested = node->AsMap()->FindChild("nested_message");
    ASSERT_TRUE(nested);

    auto map = nested->AsMap()->FindChild("unknown_map");
    ASSERT_TRUE(map);
    auto child = map->AsMap()->FindChild("unknown_int");
    EXPECT_EQ(1, child->AsInt64()->GetValue());
    EXPECT_FALSE(nested->AsMap()->FindChild("unknown_int"));
}

TEST_P(TSetAttributeTest, UnknownYsonNestedFieldsByPath)
{
    NYson::TProtobufWriterOptions options;
    options.UnknownYsonFieldModeResolver = [] (const NYPath::TYPath&) {
        return NYson::EUnknownYsonFieldsMode::Keep;
    };

    auto node1 = NYTree::ConvertToNode(1);
    auto node2 = NYTree::BuildYsonNodeFluently()
        .BeginList()
            .Item().Value(2)
        .EndList();

    NProto::TMessage message;
    EXPECT_THROW_WITH_SUBSTRING(
        SetProtobufFieldByPath(message, "/unknown_map/nested/key_int", node1, options),
        "/unknown_map");

    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/unknown_map/nested/key_int", node1, options, true));
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/unknown_map/nested/key_list", node2, options, true));
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/unknown_map/nested/key_list/0", node1, options));

    auto root = MessageToNode(message);
    auto map = root->AsMap()->FindChild("unknown_map");
    ASSERT_TRUE(map);
    auto nested = map->AsMap()->FindChild("nested");
    ASSERT_TRUE(nested);
    auto child = nested->AsMap()->FindChild("key_int");
    ASSERT_EQ(1, child->AsInt64()->GetValue());
    auto list = nested->AsMap()->FindChild("key_list");
    ASSERT_EQ(1, list->AsList()->FindChild(0)->AsInt64()->GetValue());
}

TEST(TSetAttributeTest, MapWithNonStringKeyWireString)
{
    TString wireStringBuffer;
    {
        NProto::TMessage message;
        message.mutable_int32_to_int32_map()->emplace(1, 1);
        message.mutable_int32_to_int32_map()->emplace(-1, 2);
        message.mutable_int32_to_int32_map()->emplace(170, 2);
        wireStringBuffer = message.SerializeAsString();
    }
    auto wireString = GetWireStringByPath(
        NProto::TMessage::descriptor(),
        TWireString::FromSerialized(wireStringBuffer), "/int32_to_int32_map");

    NProto::TMessage message;
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/int32_to_int32_map", wireString));
    auto root = MessageToNode(message);
    auto map = root->AsMap()->FindChild("int32_to_int32_map");
    auto e1 = map->AsMap()->FindChild("1");
    ASSERT_TRUE(e1);
    auto e2 = map->AsMap()->FindChild("-1");
    ASSERT_TRUE(e2);
    auto e3 = map->AsMap()->FindChild("170");
    ASSERT_TRUE(e3);
}

TEST(TSetAttributeTest, MapWithNonStringKeyYson)
{
    NProto::TMessage message;
    auto node = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("1").Value(1)
            .Item("-1").Value(2)
            .Item("170").Value(2)
        .EndMap();
    EXPECT_NO_THROW(SetProtobufFieldByPath(message, "/int32_to_int32_map", node));
    auto root = MessageToNode(message);
    auto map = root->AsMap()->FindChild("int32_to_int32_map");
    auto e1 = map->AsMap()->FindChild("1");
    ASSERT_TRUE(e1);
    auto e2 = map->AsMap()->FindChild("-1");
    ASSERT_TRUE(e2);
    auto e3 = map->AsMap()->FindChild("170");
    ASSERT_TRUE(e3);
}

TEST_P(TSetAttributeTest, ResetWithEntity)
{
    NProto::TMessage message;
    message.set_string_field("42");
    message.mutable_nested_message()->set_int32_field(42);
    message.mutable_nested_message()->add_repeated_int32_field(15);
    message.add_repeated_int32_field(5);
    message.add_repeated_int32_field(42);
    SetProtobufFieldByPath(message, "/string_field", NYTree::BuildYsonNodeFluently().Entity());
    EXPECT_EQ(message.string_field(), "");
    SetProtobufFieldByPath(message, "/nested_message/int32_field", NYTree::BuildYsonNodeFluently().Entity());
    EXPECT_EQ(message.nested_message().int32_field(), 0);
    EXPECT_EQ(message.nested_message().repeated_int32_field(0), 15);
    SetProtobufFieldByPath(message, "/repeated_int32_field/1", NYTree::BuildYsonNodeFluently().Entity());
    EXPECT_EQ(message.repeated_int32_field(0), 5);
    EXPECT_EQ(message.repeated_int32_field(1), 0);
    SetProtobufFieldByPath(message, "/repeated_int32_field", NYTree::BuildYsonNodeFluently().Entity());
    EXPECT_EQ(message.repeated_int32_field().size(), 0);
}

////////////////////////////////////////////////////////////////////////////////

class TScalarAttributesEqualitySuite
    : public ::testing::Test
{
public:
    bool AreEqual(const NYPath::TYPath& path)
    {
        return NAttributes::AreScalarAttributesEqualByPath(Message1, Message2, path);
    }

protected:
    NProto::TMessage Message1;
    NProto::TMessage Message2;
};

TEST_F(TScalarAttributesEqualitySuite, EmptyPath)
{
    EXPECT_TRUE(AreEqual(""));

    Message1.set_bool_field(true);
    Message1.set_fixed64_field(42);
    Message1.add_repeated_double_field(3.14);
    EXPECT_FALSE(AreEqual(""));

    Message2.set_bool_field(true);
    Message2.set_fixed64_field(42);
    Message2.add_repeated_double_field(3.14);

    EXPECT_TRUE(AreEqual(""));
    Message2.add_repeated_double_field(3.34);
    EXPECT_FALSE(AreEqual(""));
}

TEST_F(TScalarAttributesEqualitySuite, Simple)
{
    EXPECT_TRUE(AreEqual("/bool_field"));
    EXPECT_FALSE(Message1.has_bool_field());
    Message1.set_bool_field(false);

    // Yson nodes comparison does not consider not set field and default field values equal.
    EXPECT_FALSE(AreEqual("/bool_field"));

    Message1.set_int32_field(15);
    Message2.set_int32_field(16);
    EXPECT_FALSE(AreEqual("/int32_field"));
    Message1.set_int32_field(16);
    EXPECT_TRUE(AreEqual("/int32_field"));
}

TEST_F(TScalarAttributesEqualitySuite, Map)
{
    // It is needed because external map modifications are not synced with internal representation.
    // NB: Only calling mutable_field() would set dirty bit for synchronization with repeated field.
    auto getMap = [] (NProto::TMessage& message) -> auto& {
        return *message.mutable_string_to_int32_map();
    };
    EXPECT_TRUE(AreEqual("/string_to_int32_map"));
    getMap(Message1)["a"] = 42;
    EXPECT_FALSE(AreEqual("/string_to_int32_map"));

    getMap(Message2)["b"] = 42;
    EXPECT_FALSE(AreEqual("/string_to_int32_map"));
    getMap(Message2).erase("b");
    EXPECT_TRUE(AreEqual("/string_to_int32_map/b"));

    // Default value.
    getMap(Message1)["b"] = 0;
    EXPECT_FALSE(AreEqual("/string_to_int32_map/b"));

    getMap(Message2)["a"] = 42;
    EXPECT_FALSE(AreEqual("/string_to_int32_map"));
    EXPECT_TRUE(AreEqual("/string_to_int32_map/a"));

    getMap(Message2)["b"] = 0;
    EXPECT_TRUE(AreEqual("/string_to_int32_map"));
    EXPECT_TRUE(AreEqual("/string_to_int32_map/b"));

    // Missing values.
    EXPECT_TRUE(AreEqual("/string_to_int32_map/c"));
}

TEST_F(TScalarAttributesEqualitySuite, MapNested)
{
    auto getMap = [] (NProto::TMessage& message) -> auto& {
        return *message.mutable_nested_message_map();
    };
    EXPECT_TRUE(AreEqual("/nested_message_map"));
    EXPECT_TRUE(AreEqual("/nested_message_map/a"));
    getMap(Message1);
    EXPECT_TRUE(AreEqual("/nested_message_map"));
    getMap(Message1)["a"];
    EXPECT_FALSE(AreEqual("/nested_message_map/a"));
    EXPECT_FALSE(AreEqual("/nested_message_map"));
    getMap(Message2)["a"];
    EXPECT_TRUE(AreEqual("/nested_message_map/a"));

    getMap(Message1)["a"].set_int32_field(15);
    getMap(Message1)["a"].add_repeated_int32_field(42);
    EXPECT_FALSE(AreEqual("/nested_message_map/a"));
    getMap(Message2)["a"].set_int32_field(15);
    getMap(Message2)["a"].add_repeated_int32_field(42);
    EXPECT_TRUE(AreEqual("/nested_message_map/a"));
    getMap(Message1)["b"].set_int32_field(15);
    getMap(Message2)["b"].set_int32_field(15);
    EXPECT_TRUE(AreEqual("/nested_message_map"));
}

TEST_F(TScalarAttributesEqualitySuite, RepeatedField)
{
    EXPECT_TRUE(AreEqual("/repeated_int32_field"));
    auto& field1 = *Message1.mutable_repeated_int32_field();
    EXPECT_TRUE(AreEqual("/repeated_int32_field"));
    auto& field2 = *Message2.mutable_repeated_int32_field();

    field1.Add(5);
    EXPECT_FALSE(AreEqual("/repeated_int32_field"));
    EXPECT_FALSE(AreEqual("/repeated_int32_field/0"));
    EXPECT_TRUE(AreEqual("/repeated_int32_field/1"));

    field2.Add(11);
    EXPECT_FALSE(AreEqual("/repeated_int32_field"));
    field1.Add(11);
    field2.Add(5);
    EXPECT_FALSE(AreEqual("/repeated_int32_field"));
    field1.SwapElements(0, 1);
    EXPECT_TRUE(AreEqual("/repeated_int32_field"));

    // Invalid index.
    EXPECT_TRUE(AreEqual("/repeated_int32_field/15"));
    EXPECT_THROW_WITH_SUBSTRING(AreEqual("/repeated_int32_field/abacaba"), "index");
}

TEST_F(TScalarAttributesEqualitySuite, RepeatedFieldNested)
{
    auto& mappedField1 = *Message1.mutable_repeated_nested_message();
    auto& mappedField2 = *Message2.mutable_repeated_nested_message();

    mappedField1.Add();
    mappedField1.at(0).add_repeated_int32_field(5);
    mappedField1.at(0).mutable_nested_message()->mutable_nested_message()->set_int32_field(15);
    EXPECT_FALSE(AreEqual("/repeated_nested_message"));
    mappedField2.Add();
    EXPECT_FALSE(AreEqual("/repeated_nested_message"));
    mappedField1.at(0).clear_repeated_int32_field();
    mappedField1.at(0).clear_nested_message();
    EXPECT_TRUE(AreEqual("/repeated_nested_message"));
}

TEST_F(TScalarAttributesEqualitySuite, MessageInsideRepeated)
{
    auto& mappedField1 = *Message1.mutable_repeated_nested_message();
    auto& mappedField2 = *Message2.mutable_repeated_nested_message();

    mappedField1.Add();
    mappedField1.at(0).set_int32_field(5);
    EXPECT_FALSE(AreEqual("/repeated_nested_message/*/int32_field"));
    mappedField2.Add();
    EXPECT_FALSE(AreEqual("/repeated_nested_message/*/int32_field"));
    mappedField2.at(0).set_int32_field(5);
    EXPECT_TRUE(AreEqual("/repeated_nested_message/*/int32_field"));
    mappedField2.at(0).mutable_nested_message()->set_int32_field(3);
    EXPECT_TRUE(AreEqual("/repeated_nested_message/*/int32_field"));
}

TEST_F(TScalarAttributesEqualitySuite, IntInsideRepeated)
{
    Message1.add_repeated_int32_field(5);
    EXPECT_FALSE(AreEqual("/repeated_int32_field/*"));
    Message2.add_repeated_int32_field(3);
    EXPECT_FALSE(AreEqual("/repeated_int32_field/*"));
    Message2.mutable_repeated_int32_field()->at(0) = 5;
    EXPECT_TRUE(AreEqual("/repeated_int32_field/*"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
