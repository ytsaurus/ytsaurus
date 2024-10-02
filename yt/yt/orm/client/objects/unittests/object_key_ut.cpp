#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NOrm::NClient::NObjects::NTests {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TObjectKeyTest, Serialization)
{
    TObjectKey::TKeyFields keyFields;

    keyFields.emplace_back(127ll);
    keyFields.emplace_back(42ull);
    keyFields.emplace_back(13.0);
    keyFields.emplace_back(true);
    keyFields.emplace_back("test_str");

    TObjectKey key(keyFields);

    auto ysonString = ConvertToYsonString(key);

    auto deserializedKey = ConvertTo<TObjectKey>(ysonString);

    EXPECT_EQ(key, deserializedKey);

    ysonString = ConvertToYsonString(TObjectKey{});
    EXPECT_EQ(TObjectKey{}, ConvertTo<TObjectKey>(ysonString));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NObjects::NTests
