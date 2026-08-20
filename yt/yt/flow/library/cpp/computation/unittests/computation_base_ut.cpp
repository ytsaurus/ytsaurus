#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf VisitorDrivenClass = "NYT::NFlow::TStaticTableKeyVisitorJoiner";
constexpr TStringBuf LookupJoinerClass = "NYT::NFlow::TSimpleExternalStateJoiner";

TKeyVisitorStreamSpecPtr MakeStream(std::optional<THashSet<std::string>> externalNames)
{
    auto spec = New<TKeyVisitorStreamSpec>();
    spec->ExternalNames = std::move(externalNames);
    return spec;
}

TExternalStateJoinerSpecPtr MakeJoiner(TStringBuf className)
{
    auto spec = New<TExternalStateJoinerSpec>();
    spec->ExternalStateJoinerClassName = className;
    return spec;
}

TComputationSpecPtr MakeSpec(
    THashMap<TStreamId, TKeyVisitorStreamSpecPtr> streams,
    THashMap<std::string, TExternalStateJoinerSpecPtr> joiners)
{
    auto spec = New<TComputationSpec>();
    spec->KeyVisitorStreams = std::move(streams);
    spec->ExternalStateJoiners = std::move(joiners);
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKeyVisitorJoinerBindingsTest, RejectsSameJoinerInTwoStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"joiner"})},
        },
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_THROW(ValidateKeyVisitorJoinerBindings(*spec), std::exception);
}

TEST(TKeyVisitorJoinerBindingsTest, AcceptsSingleStream)
{
    auto spec = MakeSpec(
        {{TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})}},
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, AcceptsDifferentJoinersInDifferentStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"j1"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"j2"})},
        },
        {
            {"j1", MakeJoiner(VisitorDrivenClass)},
            {"j2", MakeJoiner(VisitorDrivenClass)},
        });
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresNonVisitorDrivenJoiner)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"lookup"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"lookup"})},
        },
        {{"lookup", MakeJoiner(LookupJoinerClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresNamesWithoutJoiner)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"manager"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"manager"})},
        },
        {});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresUnregisteredJoinerClass)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(THashSet<std::string>{"joiner"})},
            {TStreamId("s2"), MakeStream(THashSet<std::string>{"joiner"})},
        },
        {{"joiner", MakeJoiner("NYT::NFlow::TUnknownJoiner")}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

TEST(TKeyVisitorJoinerBindingsTest, IgnoresScanAllStreams)
{
    auto spec = MakeSpec(
        {
            {TStreamId("s1"), MakeStream(std::nullopt)},
            {TStreamId("s2"), MakeStream(std::nullopt)},
        },
        {{"joiner", MakeJoiner(VisitorDrivenClass)}});
    EXPECT_NO_THROW(ValidateKeyVisitorJoinerBindings(*spec));
}

////////////////////////////////////////////////////////////////////////////////

const TStreamId FiniteKeysStreamId{"finite_keys"};
const TStreamId InfiniteKeysStreamId{"infinite_keys"};
const TStreamId SourceStreamId{"source"};
const TStreamId VisitStreamId{"visit_iter"};

//! A visitor computation with two input streams and a source stream, so a narrowed wait
//! has something to leave out.
TComputationSpecPtr MakeUpstreamSpec(std::optional<THashSet<TStreamId>> upstreamStreams)
{
    auto streamSpec = New<TKeyVisitorStreamSpec>();
    streamSpec->UpstreamStreams = std::move(upstreamStreams);

    auto spec = New<TComputationSpec>();
    spec->InputStreamIds = {FiniteKeysStreamId, InfiniteKeysStreamId};
    spec->SourceStreams = {{SourceStreamId, New<TSourceSpec>()}};
    spec->KeyVisitorStreams = {{VisitStreamId, std::move(streamSpec)}};
    return spec;
}

//! |completedUpstreamStreams| lists the streams that have nothing left to deliver.
bool UpstreamCompleted(const TComputationSpecPtr& spec, const THashSet<TStreamId>& completedUpstreamStreams)
{
    return IsKeyVisitorUpstreamCompleted(*spec, VisitStreamId, completedUpstreamStreams);
}

TEST(TKeyVisitorUpstreamCompletionTest, UnsetUpstreamStreamsWaitForEveryInputAndSource)
{
    auto spec = MakeUpstreamSpec(std::nullopt);
    EXPECT_FALSE(UpstreamCompleted(spec, {}));
    EXPECT_FALSE(UpstreamCompleted(spec, {FiniteKeysStreamId, InfiniteKeysStreamId}))
        << "the source stream is an upstream too";
    EXPECT_TRUE(UpstreamCompleted(spec, {FiniteKeysStreamId, InfiniteKeysStreamId, SourceStreamId}));
}

// The point of the narrowing: a visitor retires on the end of the streams it feeds on,
// while the ones that outlive it keep running.
TEST(TKeyVisitorUpstreamCompletionTest, NarrowedSetIgnoresTheStreamsItLeftOut)
{
    auto spec = MakeUpstreamSpec(THashSet<TStreamId>{FiniteKeysStreamId});
    EXPECT_FALSE(UpstreamCompleted(spec, {InfiniteKeysStreamId, SourceStreamId}));
    EXPECT_TRUE(UpstreamCompleted(spec, {FiniteKeysStreamId}))
        << "streams outside the set must not hold the visitor back";
}

TEST(TKeyVisitorUpstreamCompletionTest, NarrowedSetWaitsForEveryStreamItNames)
{
    auto spec = MakeUpstreamSpec(THashSet<TStreamId>{FiniteKeysStreamId, SourceStreamId});
    EXPECT_FALSE(UpstreamCompleted(spec, {FiniteKeysStreamId}));
    EXPECT_FALSE(UpstreamCompleted(spec, {SourceStreamId, InfiniteKeysStreamId}));
    EXPECT_TRUE(UpstreamCompleted(spec, {FiniteKeysStreamId, SourceStreamId}));
}

// An empty set waits for nothing, so the visitor is completed from the very first check —
// the same standing as a computation with no upstream at all.
TEST(TKeyVisitorUpstreamCompletionTest, EmptySetNeverWaits)
{
    auto spec = MakeUpstreamSpec(THashSet<TStreamId>{});
    EXPECT_TRUE(UpstreamCompleted(spec, {}));
}

////////////////////////////////////////////////////////////////////////////////

class TBlockedTimeShareTest
    : public ::testing::Test
{
protected:
    static constexpr auto Window = TDuration::Minutes(10);
    static constexpr auto Epoch = TDuration::Seconds(1);

    const TInstant JobStart_ = TInstant::Seconds(1'000'000);
    const TStreamId StreamId_{"output_stream"};
    TBlockedTimeAccountant Accountant_{JobStart_};
    TInstant Now_ = JobStart_;

    //! Drives the accountant the way CheckOutputLimits does: one call per epoch,
    //! with the blocking limits of that epoch.
    void RunEpochs(int count, bool blocked)
    {
        for (int epoch = 0; epoch < count; ++epoch) {
            Now_ += Epoch;
            std::vector<TBlockedTimeAccountant::TBlockedLimit> blockedLimits;
            if (blocked) {
                blockedLimits.push_back({OutputBufferBytesLimitType, StreamId_});
            }
            Accountant_.Account(Now_, Window, blockedLimits);
        }
    }

    //! The limits as they reach TJobStatus::OutputLimits.
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> FillShares()
    {
        THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> limits;
        Accountant_.FillShares(Now_, &limits);
        return limits;
    }

    bool HasShare(TStringBuf limitType = OutputBufferBytesLimitType, const TStreamId& streamId = TStreamId("output_stream"))
    {
        auto limits = FillShares();
        auto streamShares = limits.FindPtr(limitType);
        return streamShares && streamShares->contains(streamId);
    }

    double Share(TStringBuf limitType = OutputBufferBytesLimitType, const TStreamId& streamId = TStreamId("output_stream"))
    {
        auto limits = FillShares();
        auto streamShares = limits.FindPtr(limitType);
        if (!streamShares) {
            return 0.0;
        }
        auto share = streamShares->FindPtr(streamId);
        return share ? share->BlockedTimeShare : 0.0;
    }
};

TEST_F(TBlockedTimeShareTest, YoungJobBlockedSinceItsStartReportsOne)
{
    RunEpochs(60, /*blocked*/ true);
    EXPECT_NEAR(Share(), 1.0, 0.01);
}

TEST_F(TBlockedTimeShareTest, LongLivedJobThatStartsBlockingReportsIt)
{
    // An hour of healthy work and then a stall: the share must follow the stall,
    // not stay diluted by the hour that came before it.
    RunEpochs(3600, /*blocked*/ false);
    ASSERT_EQ(Share(), 0.0);
    RunEpochs(static_cast<int>(Window.Seconds()), /*blocked*/ true);
    EXPECT_GT(Share(), 0.8);
}

TEST_F(TBlockedTimeShareTest, ShareDecaysAfterTheStallEnds)
{
    RunEpochs(3600, /*blocked*/ true);
    ASSERT_NEAR(Share(), 1.0, 0.01);

    RunEpochs(static_cast<int>(Window.Seconds()), /*blocked*/ false);
    EXPECT_LT(Share(), 0.2);
}

TEST_F(TBlockedTimeShareTest, FirstEpochMeasuresNothingYet)
{
    // The first call only establishes the reference instant: no time has passed
    // between it and the previous one, so a job blocked right away has nothing to
    // report yet — and must not conjure an empty limit entry for the stream.
    RunEpochs(1, /*blocked*/ true);
    EXPECT_FALSE(HasShare());

    RunEpochs(1, /*blocked*/ true);
    EXPECT_TRUE(HasShare());
    EXPECT_GT(Share(), 0.0);
}

TEST_F(TBlockedTimeShareTest, ChargesAreKeptApartByLimitTypeAndStream)
{
    const TStreamId otherStreamId("other_stream");
    const auto window = static_cast<int>(Window.Seconds());
    for (int epoch = 0; epoch < 2 * window; ++epoch) {
        Now_ += Epoch;
        // The whole time blocked on the output store of the other stream, and
        // only half of it on the output buffer of ours.
        std::vector<TBlockedTimeAccountant::TBlockedLimit> blocked{
            {OutputStoreBytesLimitType, otherStreamId}};
        if (epoch % 2 == 0) {
            blocked.push_back({OutputBufferBytesLimitType, StreamId_});
        }
        Accountant_.Account(Now_, Window, blocked);
    }

    EXPECT_NEAR(Share(OutputStoreBytesLimitType, otherStreamId), 1.0, 0.01);
    EXPECT_NEAR(Share(OutputBufferBytesLimitType, StreamId_), 0.5, 0.05);
    // A charge must not leak into the other limit type or the other stream.
    EXPECT_FALSE(HasShare(OutputBufferBytesLimitType, otherStreamId));
    EXPECT_FALSE(HasShare(OutputStoreBytesLimitType, StreamId_));
    EXPECT_FALSE(HasShare(ControllerLimitType, StreamId_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
