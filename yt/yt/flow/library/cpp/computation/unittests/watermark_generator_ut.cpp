#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/computation/watermark_generator.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NYPath;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

class TWatermarkGeneratorTest
    : public ::testing::Test
{
public:
    TWatermarkGeneratorTest() = default;

protected:
    IWatermarkGeneratorPtr CreateTestWatermarkGenerator()
    {
        auto spec = ConvertTo<TWatermarkGeneratorSpecPtr>(NYson::TYsonString(TStringBuf(R"""(
            {
                out_of_orderness_bound = "5s";
            }
        )""")));

        return NFlow::CreateWatermarkGenerator(spec, {}, {});
    }
};

TEST_F(TWatermarkGeneratorTest, TestUpdateAheadGenerator)
{
    auto makeMessage = [&] (const std::string& messageId, ui64 eventTimestamp) {
        TMessage message;
        message.MessageId = TMessageId(messageId);
        message.SystemTimestamp = TSystemTimestamp(1736804714ull);
        message.EventTimestamp = TSystemTimestamp(eventTimestamp);
        message.StreamId = TStreamId("test_stream");
        return message;
    };

    std::vector<std::vector<TMessage>> batches = {
        {
            makeMessage("message1", 10),
            makeMessage("message2", 20),
            makeMessage("message3", 30),
        },
        {
            makeMessage("message4", 40),
        },
        {
            makeMessage("message5", 50),
        },
        {
            makeMessage("message6", 60),
        },
        {
            makeMessage("message7_1", 70),
            makeMessage("message7_2", 80),
        },
        {
            makeMessage("message8_1", 90),
            makeMessage("message8_2", 91),
        },
    };

    std::vector<ui64> expectedRead = {
        25ull,
        35ull,
        45ull,
        55ull,
        75ull,
        86ull};
    auto stateManager = New<TStateManagerMock>();
    auto watermarkGenerator = CreateTestWatermarkGenerator();
    watermarkGenerator->Init(stateManager->CreateContext());

    EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 0ull);
    EXPECT_EQ(watermarkGenerator->GetPartitionReadWatermark(std::nullopt).Underlying(), 0ull);

    std::vector<TWatermarkGeneratorCookie> cookies;
    cookies.reserve(batches.size());
    for (int i = 0; i < std::ssize(batches); ++i) {
        cookies.push_back(watermarkGenerator->RegisterRead(batches[i]));
        EXPECT_EQ(watermarkGenerator->GetPartitionReadWatermark(std::nullopt).Underlying(), expectedRead[i]);
        EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 10ull);
    }

    std::vector<ui64> expectedPersisted = {
        35ull,
        45ull,
        55ull,
        70ull,
        86ull,
        86ull};

    for (int i = 0; i < std::ssize(batches); ++i) {
        watermarkGenerator->MarkPersisted(cookies[i]);
        EXPECT_EQ(watermarkGenerator->GetPartitionReadWatermark(std::nullopt).Underlying(), 86ull);
        EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), expectedPersisted[i]);
    }
}

TEST_F(TWatermarkGeneratorTest, TestOutOfOrderMarkPersisted)
{
    // Verify that MarkPersisted called out of order does NOT prematurely advance
    // the persisted watermark. When a non-front entry is persisted, its MaxEventTimestamp
    // is merged into the previous entry's CollapsedMax (per TBatchInfo field), and applied
    // only when that entry reaches the front and is persisted. The front's own
    // MinEventTimestamp/MaxEventTimestamp are left untouched so UpdateAhead() continues
    // to reflect the oldest in-flight batch correctly.
    auto makeMessage = [&] (ui64 eventTimestamp) {
        TMessage message;
        message.MessageId = TMessageId(std::to_string(eventTimestamp));
        message.SystemTimestamp = TSystemTimestamp(1736804714ull);
        message.EventTimestamp = TSystemTimestamp(eventTimestamp);
        message.StreamId = TStreamId("test_stream");
        return message;
    };

    auto stateManager = New<TStateManagerMock>();
    auto watermarkGenerator = CreateTestWatermarkGenerator();
    watermarkGenerator->Init(stateManager->CreateContext());

    // Read four batches: timestamps 10, 20, 30, 40.
    auto cookie0 = watermarkGenerator->RegisterRead({makeMessage(10)});
    auto cookie1 = watermarkGenerator->RegisterRead({makeMessage(20)});
    auto cookie2 = watermarkGenerator->RegisterRead({makeMessage(30)});
    auto cookie3 = watermarkGenerator->RegisterRead({makeMessage(40)});

    // Persist batch 3 (last) — non-front: batch2.CollapsedMax = max(0, 40) = 40. Erase batch3.
    // Inflight_ = [batch0(min=10,max=10,col=0), batch1(min=20,max=20,col=0), batch2(min=30,max=30,col=40)].
    // PersistedState_->Max = 0. MinAhead=10, MaxAhead=10 (front unchanged).
    // Watermark: max(0,5)-5=0. aheadMax=max(10,5)-5=5. max(0, min(10,5)) = 5.
    watermarkGenerator->MarkPersisted(cookie3);
    EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 5ull);

    // Persist batch 1 (middle) — non-front: batch0.CollapsedMax = max(0, 20) = 20. Erase batch1.
    // Inflight_ = [batch0(min=10,max=10,col=20), batch2(min=30,max=30,col=40)].
    // PersistedState_->Max = 0. MinAhead=10, MaxAhead=10 (front unchanged).
    // Watermark: max(0,5)-5=0. aheadMax=max(10,5)-5=5. max(0, min(10,5)) = 5.
    watermarkGenerator->MarkPersisted(cookie1);
    EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 5ull);

    // Persist batch 2 (non-front) — batch0.CollapsedMax = max(20, max(30,40)) = 40. Erase batch2.
    // Inflight_ = [batch0(min=10,max=10,col=40)].
    // PersistedState_->Max = 0. MinAhead=10, MaxAhead=10 (front unchanged).
    // Watermark: max(0,5)-5=0. aheadMax=max(10,5)-5=5. max(0, min(10,5)) = 5.
    watermarkGenerator->MarkPersisted(cookie2);
    EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 5ull);

    // Persist batch 0 (front) — PersistedState_->Max = max(max(0,10), CollapsedMax=40) = 40. Erase batch0.
    // Inflight_ empty. MinAhead={}, MaxAhead={}.
    // Watermark: max(40,5)-5=35. No ahead. Result = 35.
    watermarkGenerator->MarkPersisted(cookie0);
    EXPECT_EQ(watermarkGenerator->GetPartitionPersistedWatermark(std::nullopt).Underlying(), 35ull);
}

} // namespace NYT::NFlow
