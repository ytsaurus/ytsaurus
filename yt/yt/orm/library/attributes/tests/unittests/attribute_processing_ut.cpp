#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/core/yson/yson_builder.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributesDetectingConsumerTest, JustWorks)
{
    bool attributesDetected = false;
    auto consumer = CreateAttributesDetectingConsumer([&attributesDetected] () {
        attributesDetected = true;
    });

    NYTree::BuildYsonFluently(consumer.get())
        .BeginMap()
        .EndMap();

    ASSERT_FALSE(attributesDetected);

    NYTree::BuildYsonFluently(consumer.get())
        .BeginAttributes()
        .EndAttributes()
        .BeginMap()
        .EndMap();

    ASSERT_TRUE(attributesDetected);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TAttributesRemovingConsumerTest, JustWorks)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    auto consumer = CreateAttributesRemovingConsumer(builder.GetConsumer());

    NYTree::BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key").Entity()
        .EndAttributes()
        .Entity();

    ASSERT_EQ(builder.Flush().AsStringBuf(), "#");
}

TEST(TAttributesRemovingConsumerTest, NestedAttributes)
{
    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    auto consumer = CreateAttributesRemovingConsumer(builder.GetConsumer());

    NYTree::BuildYsonFluently(consumer.get())
        .BeginAttributes()
            .Item("key")
                .BeginAttributes()
                    .Item("key").Entity()
                .EndAttributes()
                .Entity()
        .EndAttributes()
        .BeginMap()
            .Item("key")
                .BeginAttributes()
                .EndAttributes()
                .Value("value")
        .EndMap();

    ASSERT_EQ(builder.Flush().AsStringBuf(), "{\"key\"=\"value\";}");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
