#include <yt/yt/orm/library/attributes/helpers.h>

#include <yt/yt/orm/library/attributes/tests/proto/scalar_attribute.pb.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/yson/yson_builder.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TIntEnumToStringYsonConsumerTest, ConvertsKnownValue)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    TIntEnumToStringYsonConsumer consumer(
        builder.GetConsumer(),
        NYson::ReflectProtobufEnumType(NProto::EColor_descriptor()));

    consumer.OnInt64Scalar(NProto::C_RED);

    EXPECT_EQ(builder.Flush().ToString(), "\"red\"");
}

TEST(TIntEnumToStringYsonConsumerTest, RejectsUnknownValue)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    TIntEnumToStringYsonConsumer consumer(
        builder.GetConsumer(),
        NYson::ReflectProtobufEnumType(NProto::EColor_descriptor()));

    EXPECT_THROW_WITH_SUBSTRING(consumer.OnInt64Scalar(42), "42");
}

TEST(TIntEnumToStringYsonConsumerTest, ForwardsEntity)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    TIntEnumToStringYsonConsumer consumer(
        builder.GetConsumer(),
        NYson::ReflectProtobufEnumType(NProto::EColor_descriptor()));

    consumer.OnEntity();

    EXPECT_EQ(builder.Flush().ToString(), "#");
}

TEST(TIntEnumToStringYsonConsumerTest, ConvertsList)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    TIntEnumToStringYsonConsumer consumer(
        builder.GetConsumer(),
        NYson::ReflectProtobufEnumType(NProto::EColor_descriptor()));

    consumer.OnBeginList();
    consumer.OnListItem();
    consumer.OnInt64Scalar(NProto::C_RED);
    consumer.OnListItem();
    consumer.OnInt64Scalar(NProto::C_GREEN);
    consumer.OnEndList();

    EXPECT_EQ(builder.Flush().ToString(), "[\"red\";\"green\";]");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
