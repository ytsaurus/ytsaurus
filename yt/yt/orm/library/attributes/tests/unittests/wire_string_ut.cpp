#include <yt/yt/orm/library/attributes/wire_string.h>

#include <yt/yt/orm/library/attributes/tests/proto/wire_string.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

using google::protobuf::internal::WireFormatLite;

////////////////////////////////////////////////////////////////////////////////

template <class TCallable, class... TArgs>
TString SerializeAsString(TCallable callable, TArgs... args)
{
    TString result;
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
    std::vector<TString> serializedWheels;
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

    std::vector<TString> serializedWeights;
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

    std::vector<TString> serializedQualityControls;
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

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
