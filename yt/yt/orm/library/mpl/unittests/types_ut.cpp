#include <yt/yt/orm/library/mpl/types.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NOrm::NMpl::NTests {

namespace {

struct TA
{
    static constexpr TStringBuf Name = "A";
};

struct TB
{
    static constexpr TStringBuf Name = "B";
};

struct TC
{
    static constexpr TStringBuf Name = "C";
};

} // namespace

TEST(TTypes, Size)
{
    static_assert(TTypes<>::Size == 0);
    static_assert(TTypes<int>::Size == 1);
    static_assert(TTypes<int, float>::Size == 2);
    static_assert(TTypes<TA, TB, TC>::Size == 3);
}

TEST(TTypes, Contains)
{
    static_assert(TTypes<>::Contains<TA> == false);
    static_assert(TTypes<TA>::Contains<TA> == true);
    static_assert(TTypes<TA>::Contains<TB> == false);
    static_assert(TTypes<TA, TB>::Contains<TA> == true);
    static_assert(TTypes<TA, TB>::Contains<TB> == true);
    static_assert(TTypes<TA, TB>::Contains<TC> == false);
}

TEST(TTypes, IndexOf)
{
    using TMyTypes = TTypes<TA, TB, TC>;
    static_assert(TMyTypes::IndexOf<TA> == 0);
    static_assert(TMyTypes::IndexOf<TB> == 1);
    static_assert(TMyTypes::IndexOf<TC> == 2);
}

TEST(TTypes, Wrap)
{
    using TPair = TTypes<TA, TB>::Wrap<std::pair>;
    [[maybe_unused]] TPair p(TA{}, TB{});

    using TTuple = TTypes<TA, TB, TC>::Wrap<std::tuple>;
    [[maybe_unused]] TTuple t(TA{}, TB{}, TC{});
}

TEST(TTypes, MapWrap)
{
    using TTuple = TTypes<TA, TB, TC>::Map<std::optional>::Wrap<std::tuple>;
    [[maybe_unused]] TTuple t(TA{}, std::nullopt, TC{});
}

TEST(TTypes, ForEach)
{
    using TMyTypes = TTypes<TA, TB, TC>;
    std::vector<std::string_view> names;

    TMyTypes::ForEach([&] <typename T> {
        names.push_back(T::Name);
    });

    ASSERT_EQ(names, std::vector<std::string_view>({"A", "B", "C"}));
}

} // namespace NYT::NOrm::NMpl::NTests
