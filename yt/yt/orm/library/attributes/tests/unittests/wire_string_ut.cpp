#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/orm/library/attributes/wire_string.h>

#include <yt/yt/orm/library/attributes/tests/proto/scalar_attribute.pb.h>
#include <yt/yt/orm/library/attributes/tests/proto/wire_string.pb.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/test_framework/framework.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

using google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

template <class TCallable, class... TArgs>
TProtoStringType SerializeAsString(TCallable callable, TArgs... args)
{
    TProtoStringType result;
    google::protobuf::io::StringOutputStream outputStream(&result);
    google::protobuf::io::CodedOutputStream codedStream(&outputStream);
    std::invoke(callable, args..., &codedStream);
    return result;
}

template <std::derived_from<NProtoBuf::MessageLite> TProtoMessage>
bool IsMessageEqualTo(const TProtoMessage& message, TWireString wireString)
{
    TProtoMessage parsedMessage;
    MergeMessageFrom(&parsedMessage, wireString);
    return google::protobuf::util::MessageDifferencer::Equals(message, parsedMessage);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWireStringTest, Equality)
{
    EXPECT_EQ(TWireString::Empty, TWireString::Empty);
    EXPECT_EQ(TWireString::Empty, TWireString::FromSerialized(""sv));
    EXPECT_EQ(TWireString::Empty, TWireString::FromSerialized({""sv, ""sv}));
    EXPECT_EQ(TWireString::FromSerialized({""sv}), TWireString::FromSerialized({""sv, ""sv}));

    EXPECT_EQ(TWireString::FromSerialized("a"sv), TWireString::FromSerialized("a"sv));
    EXPECT_EQ(TWireString::FromSerialized({"a"sv, ""sv}), TWireString::FromSerialized({"a"sv, ""sv, ""sv}));

    EXPECT_EQ(TWireString::FromSerialized("ab"sv), TWireString::FromSerialized({"a"sv, "b"sv}));
    EXPECT_EQ(TWireString::FromSerialized({"a"sv, "b"sv}), TWireString::FromSerialized("ab"sv));
    EXPECT_EQ(TWireString::FromSerialized({"a"sv, "b"sv}), TWireString::FromSerialized({"a"sv, ""sv, "b"sv}));

    EXPECT_EQ(TWireString::FromSerialized("abcde"sv), TWireString::FromSerialized({"ab"sv, "cde"sv}));
    EXPECT_EQ(TWireString::FromSerialized({"abcd"sv, "e"sv}), TWireString::FromSerialized({"ab"sv, "c"sv, "de"sv}));

    EXPECT_NE(TWireString::FromSerialized("a"sv), TWireString::FromSerialized("b"sv));
    EXPECT_NE(TWireString::FromSerialized({"ab"sv}), TWireString::FromSerialized({"ab"sv, "c"sv}));
    EXPECT_NE(TWireString::FromSerialized({"ab"sv, ""sv}), TWireString::FromSerialized({"ab"sv, "c"sv}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TGetWireStringByPathTest, Flat)
{
    NProto::TSheep sheep;
    sheep.set_name("Friendly Sheep");

    auto sheepPayload = sheep.SerializeAsString();
    TWireString sheepWireString = TWireString::FromSerialized(sheepPayload);

    EXPECT_EQ(
        GetWireStringByPath(sheep.GetDescriptor(), sheepWireString, "/name"),
        TWireString::FromSerialized("Friendly Sheep"));

    EXPECT_EQ(
        GetWireStringByPath(sheep.GetDescriptor(), sheepWireString, "/age"),
        TWireString::Empty);
}

TEST(TGetWireStringByPathTest, AttributeDictionary)
{
    NProto::TSheep sheep;
    auto sheepLabels = NYTree::IAttributeDictionary::FromMap(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("color").Value("blue")
        .EndMap()->AsMap());
    ToProto(sheep.mutable_labels(), *sheepLabels);

    auto sheepPayload = sheep.SerializeAsString();
    TWireString sheepWireString = TWireString::FromSerialized(sheepPayload);

    // TODO(grigminakov): Support GetWireStringByPath for TAttributeDictionary.
    EXPECT_THROW_WITH_ERROR_CODE(
        GetWireStringByPath(sheep.GetDescriptor(), sheepWireString, "/labels/color"),
        EErrorCode::Unimplemented);
}

TEST(TGetWireStringByPathTest, Hierarchy)
{
    NProto::TCar car;
    car.set_manufacturer("Local Car Building Company");
    car.set_model("Fastest Local Car");
    car.add_wheels()->set_radius(0.0);
    car.add_wheels()->set_radius(1.0);
    car.add_wheels()->set_radius(2.0);
    car.add_wheels()->set_radius(3.0);
    car.mutable_engine()->set_name("Fast Engine 25");
    car.mutable_engine()->set_horsepower(78);
    car.add_weights(10);
    car.add_weights(20);
    car.add_quality_controls(false);
    car.add_quality_controls(true);
    car.mutable_owner_to_experience()->emplace("Arthur", 3u);
    car.mutable_sensor_to_voltage()->emplace(10u, -3);

    auto carPayload = car.SerializeAsString();
    auto wireString = TWireString::FromSerialized(carPayload);

    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/manufacturer"),
        TWireString::FromSerialized("Local Car Building Company"));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/model"),
        TWireString::FromSerialized("Fastest Local Car"));

    NProto::TWheel wheel;
    std::vector<std::string> serializedWheels;
    for (int wheelIndex = 0; wheelIndex < 4; ++wheelIndex) {
        auto wheelWireString = GetWireStringByPath(car.descriptor(), wireString, Format("/wheels/%v", wheelIndex));
        wheel.set_radius(wheelIndex);
        serializedWheels.push_back(wheel.SerializeAsString());
        EXPECT_TRUE(IsMessageEqualTo(wheel, wheelWireString));
        EXPECT_EQ(
            wheelWireString,
            TWireString::FromSerialized(serializedWheels.back()));
    }

    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/wheels"),
        TWireString::FromSerialized(serializedWheels));

    NProto::TEngine engine;
    engine.set_name("Fast Engine 25");
    engine.set_horsepower(78);

    auto engineWireString = GetWireStringByPath(car.descriptor(), wireString, "/engine");
    EXPECT_TRUE(IsMessageEqualTo(engine, engineWireString));
    EXPECT_EQ(
        engineWireString,
        TWireString::FromSerialized(engine.SerializeAsString()));

    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/engine/name"),
        TWireString::FromSerialized("Fast Engine 25"));

    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/engine/horsepower"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteInt32NoTag, 78)));

    std::vector<std::string> serializedWeights;
    serializedWeights.push_back(SerializeAsString(&WireFormatLite::WriteUInt32NoTag, 10));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/weights/0"),
        TWireString::FromSerialized(serializedWeights.back()));
    serializedWeights.push_back(SerializeAsString(&WireFormatLite::WriteUInt32NoTag, 20));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/weights/1"),
        TWireString::FromSerialized(serializedWeights.back()));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/weights"),
        TWireString::FromSerialized(serializedWeights));

    std::vector<std::string> serializedQualityControls;
    serializedQualityControls.push_back(SerializeAsString(&WireFormatLite::WriteBoolNoTag, false));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/quality_controls/0"),
        TWireString::FromSerialized(serializedQualityControls.back()));
    serializedQualityControls.push_back(SerializeAsString(&WireFormatLite::WriteBoolNoTag, true));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/quality_controls/1"),
        TWireString::FromSerialized(serializedQualityControls.back()));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/quality_controls"),
        TWireString::FromSerialized(serializedQualityControls));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/owner_to_experience/Arthur"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteFixed32NoTag, 3)));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/sensor_to_voltage/10"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteSInt64NoTag, -3)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TGetWireStringByPathTest, MapOverwrite)
{
    NProto::TCar car;
    car.mutable_owner_to_experience()->emplace("Arthur", 0u);
    car.mutable_owner_to_experience()->emplace("Ivan", 1u);
    car.mutable_sensor_to_voltage()->emplace(10u, 0);
    car.mutable_sensor_to_voltage()->emplace(0u, -10);
    auto carPayload = car.SerializeAsString();

    NProto::TCar carOverwrite;
    carOverwrite.mutable_owner_to_experience()->emplace("Arthur", 5u);
    carOverwrite.mutable_sensor_to_voltage()->emplace(10u, 5);
    auto carOverwritePayload = carOverwrite.SerializeAsString();

    auto wireString = TWireString::FromSerialized(
        std::vector<std::string_view>{carPayload, carOverwritePayload});

    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/owner_to_experience/Arthur"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteFixed32NoTag, 5)));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/owner_to_experience/Ivan"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteFixed32NoTag, 1)));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/sensor_to_voltage/10"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteSInt64NoTag, 5)));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), wireString, "/sensor_to_voltage/0"),
        TWireString::FromSerialized(SerializeAsString(&WireFormatLite::WriteSInt64NoTag, -10)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWriteWireStringTest, AddTag)
{
    NProto::TCar car;
    car.mutable_engine()->set_name("Engine Name");

    auto serializedCar = car.SerializeAsString();
    auto serializedEngine = car.mutable_engine()->SerializeAsString();

    EXPECT_EQ(
        AddWireTag(
            NYson::ReflectProtobufMessageType(car.GetDescriptor()),
            "engine",
            serializedEngine),
        serializedCar);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWriteWireStringTest, SerializeYsonString)
{
    auto rootElement = NYson::ResolveProtobufElementByYPath(
        NYson::ReflectProtobufMessageType<NProto::TMessage>(),
        /*path*/ "").Element;
    NYson::TProtobufElement element = std::make_unique<NYson::TProtobufAnyElement>();
    const auto* fieldDescriptor = NProto::TMessage::descriptor()->FindFieldByName("yson_string_field");
    EXPECT_EQ(
        fieldDescriptor,
        FindYsonStringFieldDescriptor(rootElement, "/yson_string_field"));

    auto mapValue = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();
    EXPECT_THROW(ConvertToWireString(mapValue, element), std::exception);

    auto serializedMapValue = ConvertToWireString(mapValue, element, fieldDescriptor);
    ASSERT_EQ(1, std::ssize(serializedMapValue));
    EXPECT_EQ(
        "value",
        NYTree::ConvertToNode(NYson::TYsonString(serializedMapValue.front()))
            ->AsMap()
            ->GetChildValueOrThrow<std::string>("key"));

    for (const auto& value : {"123", "#", "", "{key=value;}"}) {
        auto node = NYTree::ConvertToNode(value);
        auto wireString = ConvertToWireString(node, element, fieldDescriptor);
        ASSERT_EQ(1, std::ssize(wireString));
        EXPECT_EQ(NYson::ConvertToYsonString(node).AsStringBuf(), wireString.front());
        EXPECT_EQ(
            value,
            NYTree::ConvertToNode(NYson::TYsonString(wireString.front()))->AsString()->GetValue());
    }
}

TEST(TWriteWireStringTest, FindYsonStringFieldDescriptor)
{
    auto rootElement = NYson::ResolveProtobufElementByYPath(
        NYson::ReflectProtobufMessageType<NProto::TMessage>(),
        /*path*/ "").Element;
    const auto* nestedFieldDescriptor =
        NProto::TNestedMessage::descriptor()->FindFieldByName("yson_string_field");
    EXPECT_EQ(
        nestedFieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/nested_message/yson_string_field"));
    EXPECT_EQ(
        nestedFieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/nested_message_map/key/yson_string_field"));
    EXPECT_EQ(
        nestedFieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_nested_message/0/yson_string_field"));

    const auto* repeatedFieldDescriptor =
        NProto::TMessage::descriptor()->FindFieldByName("repeated_yson_string_field");
    EXPECT_EQ(
        repeatedFieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_yson_string_field/end"));
    EXPECT_EQ(
        repeatedFieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_yson_string_field/before:0"));

    EXPECT_EQ(nullptr, FindYsonStringFieldDescriptor(rootElement, "/*"));
    EXPECT_EQ(
        nullptr,
        FindYsonStringFieldDescriptor(rootElement, "/nested_message/*"));
    EXPECT_EQ(
        nullptr,
        FindYsonStringFieldDescriptor(rootElement, "/nested_message_map/*"));
    EXPECT_EQ(
        nullptr,
        FindYsonStringFieldDescriptor(rootElement, "/repeated_nested_message/*"));
    EXPECT_EQ(
        nullptr,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_yson_string_field/before:not-an-index"));
}

TEST(TWriteWireStringTest, SerializeRepeatedYsonString)
{
    auto rootType = NYson::ReflectProtobufMessageType<NProto::TMessage>();
    auto rootElement = NYson::ResolveProtobufElementByYPath(rootType, /*path*/ "").Element;
    auto element = NYson::ResolveProtobufElementByYPath(rootType, "/repeated_yson_string_field");
    const auto* fieldDescriptor = NProto::TMessage::descriptor()->FindFieldByName(
        "repeated_yson_string_field");
    EXPECT_EQ(
        fieldDescriptor,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_yson_string_field/0"));
    EXPECT_EQ(
        nullptr,
        FindYsonStringFieldDescriptor(
            rootElement,
            "/repeated_yson_string_field/0/key"));
    auto listValue = NYTree::BuildYsonNodeFluently()
        .BeginList()
            .Item().Value("123")
            .Item().BeginMap()
                .Item("key").Value("value")
            .EndMap()
        .EndList();

    auto serializedValues = ConvertToWireString(listValue, element.Element, fieldDescriptor);

    ASSERT_EQ(2, std::ssize(serializedValues));
    EXPECT_EQ(
        NYson::ConvertToYsonString(listValue->AsList()->GetChildOrThrow(0)).AsStringBuf(),
        serializedValues[0]);
    EXPECT_EQ(
        NYson::ConvertToYsonString(listValue->AsList()->GetChildOrThrow(1)).AsStringBuf(),
        serializedValues[1]);
}

TEST(TWriteWireStringTest, DeserializeYsonString)
{
    NYson::TProtobufElement element = std::make_unique<NYson::TProtobufAnyElement>();
    auto wireString = TWireString::FromSerialized("{key=value;}"sv);
    const auto* fieldDescriptor = NProto::TMessage::descriptor()->FindFieldByName("yson_string_field");

    EXPECT_THROW(ConvertProtobufElementToNode(element, wireString), std::exception);

    auto value = ConvertProtobufElementToNode(element, wireString, fieldDescriptor);

    EXPECT_EQ("value", value->AsMap()->GetChildValueOrThrow<std::string>("key"));
}

TEST(TWriteWireStringTest, DeserializeRepeatedYsonString)
{
    auto rootType = NYson::ReflectProtobufMessageType<NProto::TMessage>();
    auto element = NYson::ResolveProtobufElementByYPath(rootType, "/repeated_yson_string_field");
    const auto* fieldDescriptor = NProto::TMessage::descriptor()->FindFieldByName(
        "repeated_yson_string_field");
    auto wireString = TWireString::FromSerialized({"{key=value;}"sv, ""sv});

    auto value = ConvertProtobufElementToNode(element.Element, wireString, fieldDescriptor)->AsList();

    ASSERT_EQ(2, value->GetChildCount());
    EXPECT_EQ("value", value->GetChildOrThrow(0)->AsMap()->GetChildValueOrThrow<std::string>("key"));
    EXPECT_EQ(NYTree::ENodeType::Entity, value->GetChildOrThrow(1)->GetType());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWriteWireStringTest, SerializeMap)
{
    NProto::TCar car;
    car.mutable_owner_to_experience()->emplace("Arthur", 0u);
    car.mutable_sensor_to_voltage()->emplace(6, 220);
    (*car.mutable_series_to_engine())[1998].set_name("Engine V1");

    auto serializedCar = car.SerializeAsString();
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), TWireString::FromSerialized(serializedCar), "/owner_to_experience"),
        TWireString::FromSerialized(SerializeKeyValuePair(
            TWireStringPart::FromStringView("Arthur"sv),
            NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_STRING},
            TWireString::FromSerialized(
                SerializeUint64(0u, NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_FIXED32})),
            NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_FIXED32})));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), TWireString::FromSerialized(serializedCar), "/sensor_to_voltage"),
        TWireString::FromSerialized(
            SerializeKeyValuePair(
                TWireStringPart::FromStringView(
                    SerializeUint64(6u, NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_FIXED32})),
                NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_FIXED32},
                TWireString::FromSerialized(
                    SerializeInt64(220, NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_SINT64})),
                NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_SINT64})));
    EXPECT_EQ(
        GetWireStringByPath(car.descriptor(), TWireString::FromSerialized(serializedCar), "/series_to_engine"),
        TWireString::FromSerialized(
            SerializeKeyValuePair(
                TWireStringPart::FromStringView(
                    SerializeInt64(1998, NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_INT32})),
                NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_INT32},
                TWireString::FromSerialized(car.series_to_engine().at(1998).SerializeAsString()),
                NYson::TProtobufElementType{WireFormatLite::FieldType::TYPE_MESSAGE})));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
