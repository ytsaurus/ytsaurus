#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/orm/library/attributes/unwrapping_consumer.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TUnwrappingConsumerTest, OnRawSupport)
{
    TYsonStringWriterHelper helper(NYson::EYsonFormat::Binary, NYson::EYsonType::MapFragment);
    TUnwrappingConsumer consumer(helper.GetConsumer());
    consumer.OnRaw(R"({"key"="value";})", NYson::EYsonType::Node);
    ASSERT_EQ(helper.Flush().ToString(), R"("key"="value";)");
}

TEST(TUnwrappingConsumerTest, OnRawSupportWithTrailingSpaces)
{
    TYsonStringWriterHelper helper(NYson::EYsonFormat::Binary, NYson::EYsonType::MapFragment);
    TUnwrappingConsumer consumer(helper.GetConsumer());
    consumer.OnRaw(R"(       {"key"="value";}   )", NYson::EYsonType::Node);
    ASSERT_EQ(helper.Flush().ToString(), R"("key"="value";)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
