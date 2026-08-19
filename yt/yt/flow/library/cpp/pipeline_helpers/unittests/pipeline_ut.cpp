#include <yt/yt/flow/library/cpp/pipeline_helpers/pipeline.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;

using ::testing::_;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TWaitPipelineStateTest, PassesExplicitRequestTimeout)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_TRUE(options.Timeout);
            EXPECT_EQ(options.Timeout.value_or(TDuration::Zero()), TDuration::Seconds(1));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1),
        TDuration::Seconds(1));
}

TEST(TWaitPipelineStateTest, UsesDefaultRequestTimeout)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_TRUE(options.Timeout);
            EXPECT_EQ(options.Timeout.value_or(TDuration::Zero()), TDuration::Seconds(60));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1));
}

TEST(TWaitPipelineStateTest, ClampsRequestTimeoutToRemainingWaitBudget)
{
    auto client = New<StrictMock<TMockClient>>();
    const auto waitTimeout = TDuration::Hours(1);
    std::optional<TDuration> actualTimeout;

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([&] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            actualTimeout = options.Timeout;
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    const auto started = TInstant::Now();
    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        waitTimeout,
        TDuration::Days(1));
    const auto elapsed = TInstant::Now() - started;

    ASSERT_TRUE(actualTimeout);
    EXPECT_LE(*actualTimeout, waitTimeout);
    EXPECT_GE(*actualTimeout, waitTimeout - elapsed);
}

TEST(TWaitPipelineStateTest, RetriesFailedRequest)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(2)
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_EQ(options.Timeout, TDuration::Seconds(1));
            return MakeFuture<TPipelineState>(TError("Transient state request failure"));
        })
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            EXPECT_EQ(options.Timeout, TDuration::Seconds(1));
            return MakeFuture(TPipelineState{.State = EPipelineState::Stopped});
        });

    WaitPipelineState(
        client,
        "//tmp/pipeline",
        EPipelineState::Stopped,
        TDuration::Hours(1),
        TDuration::Seconds(1));
}

TEST(TWaitPipelineStateTest, PropagatesLastErrorAfterRetryLimit)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(10)
        .WillRepeatedly([] (const NYPath::TYPath&, const TGetPipelineStateOptions&) {
            return MakeFuture<TPipelineState>(TError("Persistent state request failure"));
        });

    EXPECT_THROW_WITH_SUBSTRING(
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::Hours(1),
            TDuration::Seconds(1)),
        "Persistent state request failure");
}

TEST(TWaitPipelineStateTest, AttachesLastErrorWhenWaitDeadlineExpires)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .WillOnce([] (const NYPath::TYPath&, const TGetPipelineStateOptions& options) {
            Sleep(options.Timeout.value_or(TDuration::Zero()) + TDuration::MilliSeconds(10));
            return MakeFuture<TPipelineState>(TError("State request failed at the deadline"));
        });

    try {
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::MilliSeconds(500),
            TDuration::Seconds(1));
        ADD_FAILURE() << "WaitPipelineState did not throw";
    } catch (const TErrorException& ex) {
        EXPECT_EQ(ex.Error().GetMessage(), "Wait timed out");
        ASSERT_EQ(ex.Error().InnerErrors().size(), 1u);
        EXPECT_EQ(ex.Error().InnerErrors()[0].GetMessage(), "State request failed at the deadline");
    }
}

TEST(TWaitPipelineStateTest, ZeroWaitDoesNotIssueRequest)
{
    auto client = New<StrictMock<TMockClient>>();

    EXPECT_CALL(*client, GetPipelineState("//tmp/pipeline", _))
        .Times(0);

    EXPECT_THROW_WITH_SUBSTRING(
        WaitPipelineState(
            client,
            "//tmp/pipeline",
            EPipelineState::Stopped,
            TDuration::Zero()),
        "Wait timed out");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
