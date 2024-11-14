#include <yt/yt/orm/library/attributes/patch_unwrapping_consumer.h>
#include <yt/yt/orm/library/attributes/yson_builder.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TPatchUnwrappingConsumerTest, SimpleUnwrapping)
{
    TYsonStringBuilder helper(EYsonFormat::Text, EYsonType::MapFragment, /*enableRaw*/ false);
    TPatchUnwrappingConsumer consumer(helper.GetConsumer());

    BuildYsonFluently(&consumer)
        .BeginMap()
            .Item("key").Value("value")
        .EndMap();

    auto expected = BuildYsonStringFluently<EYsonType::MapFragment>(EYsonFormat::Text)
        .Item("key").Value("value")
        .Finish();

    ASSERT_EQ(helper.Flush().AsStringBuf(), expected.AsStringBuf());
}

TEST(TPatchUnwrappingConsumerTest, Forwarding)
{
    TYsonStringBuilder helper(EYsonFormat::Text, EYsonType::MapFragment, /*enableRaw*/ false);
    TPatchUnwrappingConsumer consumer(helper.GetConsumer());

    BuildYsonFluently(&consumer)
        .BeginAttributes()
            .Item("attr").Entity()
        .EndAttributes()
        .BeginMap()
            .Item("key1").Value("value")
            .Item("key2").Entity()
        .EndMap();

    auto expected = BuildYsonStringFluently<EYsonType::MapFragment>(EYsonFormat::Text)
        .Item("key1")
            .BeginAttributes()
                .Item("attr").Entity()
            .EndAttributes()
            .Value("value")
        .Item("key2")
            .BeginAttributes()
                .Item("attr").Entity()
            .EndAttributes()
            .Entity()
        .Finish();

    ASSERT_EQ(helper.Flush().AsStringBuf(), expected.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
