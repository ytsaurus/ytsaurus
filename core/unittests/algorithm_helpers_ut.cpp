#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/algorithm_helpers.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(AlgorithmHelpers, BinarySearch) {
    {
        std::vector<TString> v;
        auto it = LowerBound(v.begin(), v.end(), "test");
        EXPECT_EQ(it, v.end());
    }

    {
        int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        auto it = LowerBound(data, data + Y_ARRAY_SIZE(data), 2);
        EXPECT_EQ(it - data, 1);

        it = LowerBound(data, data + Y_ARRAY_SIZE(data), 10);
        EXPECT_EQ(it, data + Y_ARRAY_SIZE(data));
    }

    {
        std::vector<size_t> data = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0};

        {
            auto it = BinarySearch(data.begin(), data.end(), [] (auto it) {
                return *it > 9;
            });
            EXPECT_EQ(it, data.begin() + 1);
        }
        {
            auto it = BinarySearch(data.begin(), data.end(), [] (auto it) {
                return *it > 11;
            });
            EXPECT_EQ(it, data.begin());
        }
        {
            auto it = LowerBound(data.rbegin(), data.rend(), 1);
            EXPECT_EQ(it, data.rbegin() + 1);
        }
    }
}

TEST(AlgorithmHelpers, ExponentialSearch) {
    {
        int data[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        for (int i = 0; i < 11; ++i) {
            auto it = ExpLowerBound(data, data + Y_ARRAY_SIZE(data), i);
            EXPECT_EQ(it, data + (i > 0 ? i - 1 : 0));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
