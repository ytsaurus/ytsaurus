#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>
#include <yt/core/yson/stream.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

namespace NYT {
namespace NYson {
namespace {

using namespace NYTree;
using NJobTrackerClient::TStatistics;
using NJobTrackerClient::TSummary;

////////////////////////////////////////////////////////////////////////////////

TEST(TYsonTest, ConvertToUsingBuildingYsonConsumer)
{
    TYsonString statisticsYson(
        "{"
            "abc="
            "{"
                "def="
                "{"
                    "sum=42; count=3; min=5; max=21;"
                "};"
                "degh="
                "{"
                    "sum=27; count=1; min=27; max=27;"
                "};"
            "};"
            "xyz="
            "{"
                "sum=50; count=5; min=8; max=12;"
            "};"
        "}");
    auto statistics = ConvertTo<TStatistics>(statisticsYson);
    auto data = statistics.Data();
    
    std::map<TString, TSummary> expectedData {
        { "/abc/def", TSummary(42, 3, 5, 21) },
        { "/abc/degh", TSummary(27, 1, 27, 27) },
        { "/xyz", TSummary(50, 5, 8, 12) },
    };

    EXPECT_EQ(expectedData, data);
}

} // namespace
} // namespace NYson
} // namespace NYT
