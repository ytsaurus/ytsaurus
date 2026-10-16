#include <yt/yt/flow/library/cpp/misc/bipartite_map.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestBipartiteMapTest, TBasicTest)
{
    TBipartiteMap<int, std::string> storage;

    EXPECT_TRUE(storage.Empty());
    EXPECT_TRUE(storage.Size() == 0);

    auto pair11 = std::pair{1, 1};
    auto pair12 = std::pair{1, 2};
    auto pair21 = std::pair{2, 1};
    auto pair22 = std::pair{2, 2};

    storage.AddPair(pair11, "pair11");
    storage.AddPair(pair12, "pair12");
    storage.AddPair(pair21, "pair21");
    storage.AddPair(pair22, "pair22");

    EXPECT_TRUE(storage[pair11] == "pair11");
    EXPECT_TRUE(storage.Size() == 4);

    storage.RemoveElement(1, EBipartiteMapElementType::First);

    // pair11 and pair12 are removed.
    EXPECT_TRUE(storage.Size() == 2);

    storage.RemoveElement(1, EBipartiteMapElementType::Second);

    // pair21 is removed.
    EXPECT_TRUE(storage.Size() == 1);
    EXPECT_TRUE(storage[pair22] == "pair22");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
