#include <yt/yt/orm/library/mpl/map.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <vector>

namespace NYT::NOrm::NMpl::NTests {

namespace {

template <class T>
using TContainerA = std::vector<T>;

template <class T>
struct TContainerB
{
    std::vector<T> Items;

    explicit TContainerB(std::vector<T> items)
        : Items(std::move(items))
    { }

    auto operator<=>(const TContainerB&) const = default;
};

} // namespace

TEST(TTypeToValueMapTest, DefaultConstructible)
{
    struct TKeyA { };
    struct TKeyB { };
    using TKeys = TTypes<TKeyA, TKeyB>;

    TTypeToValueMap<TKeys, int> map;
    map.Set<TKeyA>(5);
    map.Set<TKeyB>(10);

    EXPECT_EQ(map.Get<TKeyA>(), 5);
    EXPECT_EQ(map.Get<TKeyB>(), 10);
}

TEST(TTypeToValueMapTest, NotDefaultConstructible)
{
    struct TKeyA { };
    struct TKeyB { };
    using TKeys = TTypes<TKeyA, TKeyB>;

    TTypeToValueMap<TKeys, int> map([] <class T> {
        if constexpr (std::same_as<T, TKeyA>) {
            return 5;
        } else if constexpr (std::same_as<T, TKeyB>) {
            return 10;
        }
    });

    EXPECT_EQ(map.Get<TKeyA>(), 5);
    EXPECT_EQ(map.Get<TKeyB>(), 10);
}

TEST(TTypeToTemplateValueMapTest, DefaultConstructible)
{
    using TKeys = TTypes<int, std::string>;

    TTypeToTemplateValueMap<TKeys, TContainerA> map;
    map.Set<int>({1, 2, 3});
    map.Set<std::string>({"one", "two", "three"});

    EXPECT_EQ(map.Get<int>(), TContainerA<int>({1, 2, 3}));
    EXPECT_EQ(map.Get<std::string>(), TContainerA<std::string>({"one", "two", "three"}));
}

TEST(TTypeToTemplateValueMapTest, NotDefaultConstructible)
{
    using TKeys = TTypes<int, std::string>;

    TTypeToTemplateValueMap<TKeys, TContainerB> map([] <class T> {
        if constexpr (std::same_as<T, int>) {
            return TContainerB<int>({1, 2, 3});
        } else if constexpr (std::same_as<T, std::string>) {
            return TContainerB<std::string>({"one", "two", "three"});
        }
    });

    EXPECT_EQ(map.Get<int>(), TContainerB<int>({1, 2, 3}));
    EXPECT_EQ(map.Get<std::string>(), TContainerB<std::string>({"one", "two", "three"}));
}

} // namespace NYT::NOrm::NMpl::NTests
