#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/histogram.h>

#include <numeric>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(THistogramTest, Simple)
{
    auto h = CreateHistogram();

    for (i64 i : {1, 10, 100, 100}) {
        h->AddValue(i);
    }
    h->BuildHistogramView();
    auto v = h->GetHistogramView();
    EXPECT_EQ(1, v.Min);
    EXPECT_EQ(101, v.Max);
    EXPECT_EQ(100, v.Count.size());
    EXPECT_EQ(4, std::accumulate(v.Count.begin(), v.Count.end(), 0));
    EXPECT_EQ(1, v.Count.front());
    EXPECT_EQ(2, v.Count.back());
    for (i64 i : {-49, -48, 149, 150}) {
        h->AddValue(i);
    }
    h->BuildHistogramView();
    v = h->GetHistogramView();
    EXPECT_EQ(-49, v.Min);
    EXPECT_EQ(151, v.Max);
    EXPECT_EQ(200, v.Count.size());
    EXPECT_EQ(8, std::accumulate(v.Count.begin(), v.Count.end(), 0));
    EXPECT_EQ(1, v.Count.front());
    EXPECT_EQ(1, v.Count.back());
    for (i64 i : {150, 151}) {
        h->AddValue(i);
    }
    h->BuildHistogramView();
    v = h->GetHistogramView();
    EXPECT_EQ(-49, v.Min);
    EXPECT_EQ(152, v.Max);
    EXPECT_EQ(67, v.Count.size());
    EXPECT_EQ(10, std::accumulate(v.Count.begin(), v.Count.end(), 0));
    EXPECT_EQ(2, v.Count.front());
    EXPECT_EQ(4, v.Count.back());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

