#include <yt/yt/orm/library/attributes/yson_builder.h>

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NServer::NObjects::NTests {

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonStringBuilderTest, Simple)
{
    NAttributes::TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x16some_scalar"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, Recycling)
{
    NAttributes::TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar1");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x18some_scalar1"});
    ASSERT_TRUE(builder.IsEmpty());

    builder->OnStringScalar("some_scalar2");
    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x18some_scalar2"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, Checkpoints)
{
    NAttributes::TYsonStringBuilder builder;
    builder->OnStringScalar("some_scalar");

    auto checkpoint = builder.CreateCheckpoint();
    builder.CreateCheckpoint();
    builder.RestoreCheckpoint(checkpoint);
    builder.RestoreCheckpoint(checkpoint);

    ASSERT_EQ(builder.Flush().ToString(), TString{"\1\x16some_scalar"});
    ASSERT_TRUE(builder.IsEmpty());
}

TEST(TYsonStringBuilderTest, MapCheckpoints)
{
    NAttributes::TYsonStringBuilder builder(NYson::EYsonFormat::Text);

    builder->OnBeginMap();

    builder->OnKeyedItem("key1");
    builder->OnEntity();

    auto checkpoint = builder.CreateCheckpoint();
    builder->OnKeyedItem("key2");
    builder->OnEntity();
    builder.RestoreCheckpoint(checkpoint);

    builder->OnEndMap();

    ASSERT_EQ(builder.Flush().ToString(), TString{R"({"key1"=#;})"});
}

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonBuilderTest, Forwarding)
{
    NAttributes::TYsonStringBuilder stringBuilder(NYson::EYsonFormat::Text);
    NAttributes::TYsonBuilder builder(
        NAttributes::EYsonBuilderForwardingPolicy::Forward,
        &stringBuilder,
        stringBuilder.GetConsumer());
    builder->OnBeginMap();
    auto checkpoint = builder.CreateCheckpoint();
    builder->OnKeyedItem("key");
    builder->OnEntity();
    builder.RestoreCheckpoint(checkpoint);
    builder->OnEndMap();

    ASSERT_EQ(stringBuilder.Flush().ToString(), TString{R"({})"});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects::NTests
