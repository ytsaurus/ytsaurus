#include <gtest/gtest.h>

#include <yt/yt/library/profiling/tag.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTagSet, Api)
{
    TTagSet s1{{TTag{"foo", "bar"}}};
    TTagSet s2{{TTag{"foo", "zog"}}};
}

TEST(TTagSet, Subsets)
{
    TTagIdList tags{1, 2, 3};

    auto listProjections = [&] (
        TTagIndexList parents,
        TTagIndexList required,
        TTagIndexList excluded)
    {
        std::vector<TTagIdList> subsets;
        RangeSubsets(tags, parents, required, excluded, [&subsets] (auto list) {
            subsets.push_back(list);
        });

        std::sort(subsets.begin(), subsets.end());
        return subsets;
    };

    TTagIndexList noParents = {NoParentSentinel, NoParentSentinel, NoParentSentinel};

    auto full = listProjections(noParents, {}, {});
    ASSERT_EQ(static_cast<size_t>(8), full.size());

    {
        auto actual = listProjections({NoParentSentinel, 0, 1}, {}, {});
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections({NoParentSentinel, 0, 0}, {}, {});
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
            { 1, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, {0}, {});
        std::vector<TTagIdList> expected = {
            { 1 },
            { 1, 2 },
            { 1, 2, 3 },
            { 1, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, {0, 1}, {});
        std::vector<TTagIdList> expected = {
            { 1, 2 },
            { 1, 2, 3 },
        };
        ASSERT_EQ(expected, actual);
    }

    {
        auto actual = listProjections(noParents, {}, {2});
        std::vector<TTagIdList> expected = {
            { },
            { 1 },
            { 1, 2 },
            { 2 },
        };
        ASSERT_EQ(expected, actual);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
