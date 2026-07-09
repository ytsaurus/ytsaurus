#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TTraverseTest, MergeStreamTraverseData)
{
    std::vector<TStreamTraverseDataPtr> streams = {
        ConvertTo<TStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                epoch = 1;
                state = drained;
                system_watermark = 1712182928;
                event_watermark = 1712182911;
            }
        )"""))),
        ConvertTo<TStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                epoch = 2;
                state = completed;
                system_watermark = 1712182900;
                event_watermark = 1712182903;
            }
        )"""))),
    };

    auto merged = MergeStreamTraverseData(streams, EInflightMerge::None);
    ASSERT_EQ(merged->Epoch, 1);
    ASSERT_EQ(merged->State, EStreamState::Drained);
    ASSERT_EQ(merged->SystemWatermark, TSystemTimestamp(1712182900));
    ASSERT_EQ(merged->EventWatermark, TSystemTimestamp(1712182903));
}

TEST(TTraverseTest, MergeInflightTraverseData)
{
    std::vector<TInflightStreamTraverseDataPtr> inflight = {
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_system_timestamp = 1712182928;
                min_event_timestamp = 1712182911;
            }
        )"""))),
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_event_timestamp = 1712182910;
            }
        )"""))),
        ConvertTo<TInflightStreamTraverseDataPtr>(TYsonString(TStringBuf(R"""(
            {
                min_system_timestamp = 1712182929;
            }
        )"""))),
    };
    auto merged = MergeInflightTraverseData(inflight);
    ASSERT_EQ(merged->MinSystemTimestamp, TSystemTimestamp(1712182928));
    ASSERT_EQ(merged->MinEventTimestamp, TSystemTimestamp(1712182910));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
