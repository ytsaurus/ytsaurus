#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/sync_cache.h>


namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSimpleLruCache, Common)
{
    TSimpleLruCache<TString, int> cache(10);
    cache.Insert("a", 1);
    cache.Insert("b", 2);

    EXPECT_TRUE(cache.Find("a").has_value());
    EXPECT_TRUE(cache.Find("b").has_value());
    EXPECT_FALSE(cache.Find("c").has_value());
    EXPECT_EQ(cache.Get("a"), 1);
    EXPECT_EQ(cache.Get("b"), 2);

    cache.Insert("c", 3);
    
    EXPECT_FALSE(cache.Find("a").has_value());
    EXPECT_TRUE(cache.Find("b").has_value());
    EXPECT_TRUE(cache.Find("c").has_value());
    EXPECT_EQ(cache.Get("b"), 2);
    EXPECT_EQ(cache.Get("c"), 3);
    
    cache.Insert("b", 4);
    
    EXPECT_FALSE(cache.Find("a").has_value());
    EXPECT_TRUE(cache.Find("b").has_value());
    EXPECT_TRUE(cache.Find("c").has_value());
    EXPECT_EQ(cache.Get("c"), 3);
    EXPECT_EQ(cache.Get("b"), 4);
    
    cache.Insert("a", 5);

    EXPECT_TRUE(cache.Find("a").has_value());
    EXPECT_TRUE(cache.Find("b").has_value());
    EXPECT_FALSE(cache.Find("c").has_value());
    EXPECT_EQ(cache.Get("a"), 5);
    EXPECT_EQ(cache.Get("b"), 4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
