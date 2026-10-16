#include <yt/yt/server/queue_agent/ytree_helpers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NQueueAgent {
namespace {

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> YPathListSorted(
    const IYPathServicePtr& service,
    const TYPath& path,
    std::optional<i64> limit = {})
{
    auto keys = WaitForFast(AsyncYPathList(service, path, limit))
        .ValueOrThrow();
    std::ranges::sort(keys);
    return keys;
}

bool YPathExists(const IYPathServicePtr& service, const TYPath& path)
{
    return WaitForFast(AsyncYPathExists(service, path))
        .ValueOrThrow();
}

TYsonString YPathGet(const IYPathServicePtr& service, const TYPath& path)
{
    return WaitForFast(AsyncYPathGet(service, path))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

//! A part backed by an in-memory key -> integer map. The key #throwingKey, if present,
//! makes FindItemService throw to exercise the fail-fast contract.
class TIntMapPart
    : public TVirtualMapPartBase
{
public:
    TIntMapPart(THashMap<std::string, int> items, std::optional<std::string> throwingKey = {})
        : Items_(std::move(items))
        , ThrowingKey_(std::move(throwingKey))
    { }

    i64 GetSize() const override
    {
        return std::ssize(Items_);
    }

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        std::vector<std::string> keys;
        for (const auto& [key, value] : Items_) {
            if (std::ssize(keys) >= limit) {
                break;
            }
            keys.push_back(key);
        }
        return keys;
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        if (ThrowingKey_ && key == *ThrowingKey_) {
            THROW_ERROR_EXCEPTION("Lookup of key %Qv failed", key);
        }
        auto it = Items_.find(key);
        if (it == Items_.end()) {
            return nullptr;
        }
        int value = it->second;
        return IYPathService::FromProducer(BIND([value] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer).Value(value);
        }));
    }

private:
    const THashMap<std::string, int> Items_;
    const std::optional<std::string> ThrowingKey_;
};

TVirtualMapPartBasePtr MakePart(THashMap<std::string, int> items, std::optional<std::string> throwingKey = {})
{
    return New<TIntMapPart>(std::move(items), std::move(throwingKey));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMergedVirtualMapServiceTest, Union)
{
    auto service = CreateMergedVirtualMapService({
        MakePart({{"a", 1}, {"b", 2}}),
        MakePart({{"c", 3}}),
    });

    EXPECT_EQ((std::vector<std::string>{"a", "b", "c"}), YPathListSorted(service, ""));
    EXPECT_TRUE(YPathExists(service, "/a"));
    EXPECT_TRUE(YPathExists(service, "/c"));
    EXPECT_FALSE(YPathExists(service, "/missing"));
    EXPECT_EQ(ConvertToYsonString(1, EYsonFormat::Binary), YPathGet(service, "/a"));
    EXPECT_EQ(ConvertToYsonString(3, EYsonFormat::Binary), YPathGet(service, "/c"));
    EXPECT_THROW(YPathGet(service, "/missing"), std::exception);

    // Aggregated size is reflected in the root map cardinality.
    auto root = ConvertToNode(YPathGet(service, ""))->AsMap();
    EXPECT_EQ(3, root->GetChildCount());

    // Degenerate: no parts.
    auto empty = CreateMergedVirtualMapService({});
    EXPECT_EQ((std::vector<std::string>{}), YPathListSorted(empty, ""));
    EXPECT_FALSE(YPathExists(empty, "/anything"));
    EXPECT_EQ(0, ConvertToNode(YPathGet(empty, ""))->AsMap()->GetChildCount());
}

TEST(TMergedVirtualMapServiceTest, ListLimitAcrossParts)
{
    auto service = CreateMergedVirtualMapService({
        MakePart({{"a", 1}, {"b", 2}}),
        MakePart({{"c", 3}, {"d", 4}}),
    });

    // The limit is honored across parts; exactly #limit keys are returned.
    EXPECT_EQ(3, std::ssize(WaitForFast(AsyncYPathList(service, "", /*limit*/ 3)).ValueOrThrow()));
    EXPECT_EQ(1, std::ssize(WaitForFast(AsyncYPathList(service, "", /*limit*/ 1)).ValueOrThrow()));
}

TEST(TMergedVirtualMapServiceTest, ThrowingPartIsFailFast)
{
    auto service = CreateMergedVirtualMapService({
        MakePart({{"boom", 0}, {"good", 1}}, /*throwingKey*/ "boom"),
        MakePart({{"other", 2}}),
    });

    // A key served by a throwing part fails the request (fail-fast).
    EXPECT_THROW(YPathGet(service, "/boom"), std::exception);
    EXPECT_THROW(YPathExists(service, "/boom"), std::exception);

    // Keys owned by healthy parts remain reachable: the throwing part returns null for
    // keys it does not own and thus does not poison unrelated lookups.
    EXPECT_EQ(ConvertToYsonString(1, EYsonFormat::Binary), YPathGet(service, "/good"));
    EXPECT_EQ(ConvertToYsonString(2, EYsonFormat::Binary), YPathGet(service, "/other"));
    EXPECT_FALSE(YPathExists(service, "/missing"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueAgent
