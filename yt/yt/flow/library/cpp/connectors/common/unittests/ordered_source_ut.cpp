#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/source_context.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

// Represents one parsed offset: one cookie and one message (our test source always produces one message per offset).
struct TUnpackedMessage
{
    TSourceMessageBatchCookie Cookie;
    TInputMessageConstPtr Message;
};

std::vector<TUnpackedMessage> UnpackBatches(std::vector<ISource::TMessageBatch> batches)
{
    std::vector<TUnpackedMessage> result;
    for (auto& parsed : batches) {
        EXPECT_EQ(parsed.Messages.size(), 1u);
        result.push_back({std::move(parsed.Cookie), std::move(parsed.Messages[0])});
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestSourceController);

class TTestSourceController
    : public TSourceControllerBase
{
public:
    using TSourceControllerBase::TSourceControllerBase;

    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TTestSourceController);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestMalformedMessagePolicy,
    (Keep)
    (Drop)
    (Fail)
);

//! Read behaviour of #TTestSource that mirrors the malformed message handling of real connectors.
struct TTestReadConfig
{
    THashSet<i64> MalformedOffsets;
    THashSet<i64> PayloadlessOffsets;
    ETestMalformedMessagePolicy MalformedMessagePolicy = ETestMalformedMessagePolicy::Keep;
    //! Changes #MalformedMessagePolicy in the middle of a read call.
    std::optional<ETestMalformedMessagePolicy> PolicyAfterFirstMalformed;
    bool IgnoreOffsetLimit = false;
    i64 ReadStartRewind = 0;
    //! Under `fail`, returns the records before a malformed one, as the BigRT source does, instead of
    //! an error.
    bool FailCutsRead = false;
    //! Splits reads for the connector's own reasons, whatever the batch settings.
    std::optional<i64> MaxRecordsPerCall;
    TDuration ReadCallDelay;
    //! Offsets that hold no record, as in a log whose offsets have gaps.
    THashSet<i64> AbsentOffsets;
    //! (Read call number, offset): from that call on, the log is trimmed up to the offset without a
    //! partition info update, and #TTestSourceImpl::DoFetchTrimmedOffset reports the offset.
    std::optional<std::pair<i64, i64>> TrimAtCall;
    //! Overrides the offset reported by #TTestSourceImpl::DoFetchTrimmedOffset, as a broker that trimmed
    //! past the records the connector has already received.
    std::optional<i64> ReportedTrimmedOffset;
    //! Fails #TTestSourceImpl::DoFetchTrimmedOffset, as a broker metadata request during an outage.
    TError TrimmedOffsetFetchError;
    i64 TrimmedOffsetFetchCount = 0;
    i64 ReadCallCount = 0;
    //! The start offset of every read call.
    std::vector<i64> ReadCallOffsets;
    //! The policies applied by #TTestSourceImpl::ApplyReadSettings, one per read call.
    std::vector<ETestMalformedMessagePolicy> AppliedPolicies;
};

constexpr ui64 MalformedDataShift = 1000;

//! The test connector without the replay-stable declaration.
class TTestSourceImpl
    : public TIntegerOffsetOrderedSourceBase
{
public:
    using TSourceController = TTestSourceController;
    using TOrderedSourceBase::TPartitionInfoUpdate;

    std::shared_ptr<TTestReadConfig> ReadConfig = std::make_shared<TTestReadConfig>();

    TTestSourceImpl(
        TSourceContextPtr context,
        TDynamicSourceContextPtr dynamicContext,
        std::optional<i64> initialMaxOffset = 0,
        i64 initialCommittedOffset = 0)
        : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
        , Schema_(
            New<TTableSchema>(std::vector{
                TColumnSchema("data", EValueType::Uint64),
            }))
        , MaxOffsetExclusive_(initialMaxOffset.value_or(0))
    {
        auto update = TPartitionInfoUpdate{};
        if (initialMaxOffset) {
            update.MaxOffsetExclusive = IntToOffset(*initialMaxOffset);
        }
        if (initialCommittedOffset > 0) {
            update.CommittedOffsetExclusive = IntToOffset(initialCommittedOffset);
        }
        UpdatePartitionInfo(update);
    }

    using TOrderedSourceBase::GetSourceTotalBytes;
    using TOrderedSourceBase::GetSourceTotalCount;
    using TOrderedSourceBase::UpdatePartitionInfo;

    void SetMaxOffset(i64 offset, std::optional<TInstant> updateTime = std::nullopt)
    {
        MaxOffsetExclusive_ = std::max(MaxOffsetExclusive_, offset);
        UpdatePartitionInfo(TPartitionInfoUpdate{.MaxOffsetExclusive = IntToOffset(offset), .UpdateInstant = updateTime});
    }

    void SetCommittedOffset(i64 offsetExclusive)
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{.CommittedOffsetExclusive = IntToOffset(offsetExclusive)});
    }

    void Reposition(i64 offsetExclusive)
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{
            .CommittedOffsetExclusive = IntToOffset(offsetExclusive),
            .Repositioned = true,
        });
    }

    void SetTestError(TError error)
    {
        Error_ = std::move(error);
    }

    void SetProgressRecord(i64 offsetExclusive)
    {
        ProgressOffsetExclusive_ = offsetExclusive;
    }

    void SetExtraError(TError error)
    {
        ExtraErrorState_->SetError(std::move(error));
    }

    void SetTestWriteTimestamps(std::vector<ui64> timestamps)
    {
        WriteTimestamps_ = std::move(timestamps);
    }

    void SetBacklogRate(TBacklogRate backlogRate)
    {
        BacklogRate_ = backlogRate;
    }

    std::optional<TBacklogRate> EstimateBacklogRate() override
    {
        return BacklogRate_;
    }

private:
    ETestMalformedMessagePolicy AppliedPolicy_ = ETestMalformedMessagePolicy::Keep;

    NYson::TYsonString CaptureReadSettings() override
    {
        return MakeMalformedMessagePolicyReadSettings(ReadConfig->MalformedMessagePolicy);
    }

    void ApplyReadSettings(const NYson::TYsonString& settings) override
    {
        AppliedPolicy_ = ParseMalformedMessagePolicyReadSettings(settings, ETestMalformedMessagePolicy::Keep);
        ReadConfig->AppliedPolicies.push_back(AppliedPolicy_);
    }

    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffsetAsKey,
        std::optional<TOffset> offsetLimitOptionalAsKey) final
    {
        ++ReadConfig->ReadCallCount;
        ReadConfig->ReadCallOffsets.push_back(OffsetToInt(nextOffsetAsKey));
        auto nextPosition = std::max<i64>(0, OffsetToInt(nextOffsetAsKey) - ReadConfig->ReadStartRewind);
        if (IsTrimmed()) {
            nextPosition = std::max(nextPosition, ReadConfig->TrimAtCall->second);
        }
        std::optional<i64> offsetLimitPosition;
        if (offsetLimitOptionalAsKey && !ReadConfig->IgnoreOffsetLimit) {
            offsetLimitPosition = OffsetToInt(*offsetLimitOptionalAsKey);
        }
        bool seenMalformed = false;

        ReadErrorState_->SetError(Error_);

        std::vector<TRecord> records;
        if (!Error_.IsOK()) {
            return MakeFuture(records);
        }

        TPayloadBuilder builder(Schema_);
        while (std::ssize(records) < settings->MaxRowsPerBatch &&
            std::ssize(records) < ReadConfig->MaxRecordsPerCall.value_or(std::numeric_limits<i64>::max()) &&
            nextPosition < MaxOffsetExclusive_ &&
            (!offsetLimitPosition || nextPosition < *offsetLimitPosition))
        {
            if (ReadConfig->AbsentOffsets.contains(nextPosition)) {
                ++nextPosition;
                continue;
            }

            ui64 data = nextPosition;
            bool dropped = ReadConfig->PayloadlessOffsets.contains(nextPosition);
            if (ReadConfig->MalformedOffsets.contains(nextPosition)) {
                if (AppliedPolicy_ == ETestMalformedMessagePolicy::Fail) {
                    if (ReadConfig->FailCutsRead) {
                        break;
                    }
                    return MakeFuture<std::vector<TRecord>>(TError("Critical read failure"));
                }
                if (AppliedPolicy_ == ETestMalformedMessagePolicy::Drop) {
                    dropped = true;
                } else {
                    data += MalformedDataShift;
                }
                if (!seenMalformed && ReadConfig->PolicyAfterFirstMalformed) {
                    ReadConfig->MalformedMessagePolicy = *ReadConfig->PolicyAfterFirstMalformed;
                }
                seenMalformed = true;
            }
            builder.SetValue(MakeUnversionedUint64Value(data), "data");

            auto writeTimestamp = nextPosition < std::ssize(WriteTimestamps_)
                ? TSystemTimestamp(WriteTimestamps_[nextPosition])
                : TSystemTimestamp(nextPosition + 1);

            TRecord record = {
                .Offset = IntToOffset(nextPosition),
                .WriteTimestamp = writeTimestamp,
                .CreateTimestamp = TSystemTimestamp(nextPosition + 1),
                .Payloads = {builder.Finish()},
                .PayloadSchema = builder.GetSchema(),
            };
            if (dropped) {
                record.Payloads.clear();
            }

            records.push_back(record);
            ++nextPosition;
        }
        if (ProgressOffsetExclusive_ && nextPosition >= MaxOffsetExclusive_) {
            auto continuationOffsetExclusive = *ProgressOffsetExclusive_;
            if (offsetLimitPosition) {
                continuationOffsetExclusive = std::min(continuationOffsetExclusive, *offsetLimitPosition);
            }
            if (continuationOffsetExclusive > nextPosition) {
                records.push_back(TRecord{
                    .Offset = IntToOffset(continuationOffsetExclusive - 1),
                    .WriteTimestamp = TSystemTimestamp(continuationOffsetExclusive),
                    .CreateTimestamp = TSystemTimestamp(continuationOffsetExclusive),
                    .PayloadSchema = Schema_,
                });
            }
        }
        return BIND(
            [records = std::move(records), delay = ReadConfig->ReadCallDelay] () mutable {
                for (int i = 0; i < 10; ++i) { // Imitate delay without wasting time.
                    NConcurrency::Yield();
                }
                if (delay) {
                    TDelayedExecutor::WaitForDuration(delay);
                }
                return std::move(records);
            })
            .AsyncVia(GetContext()->SerializedInvoker)
            .Run();
    }

    TFuture<std::optional<TOffset>> DoFetchTrimmedOffset() override
    {
        ++ReadConfig->TrimmedOffsetFetchCount;
        if (!ReadConfig->TrimmedOffsetFetchError.IsOK()) {
            return MakeFuture<std::optional<TOffset>>(ReadConfig->TrimmedOffsetFetchError);
        }
        if (ReadConfig->ReportedTrimmedOffset) {
            return MakeFuture(std::optional(IntToOffset(*ReadConfig->ReportedTrimmedOffset)));
        }
        return MakeFuture(std::optional(IntToOffset(IsTrimmed() ? ReadConfig->TrimAtCall->second : 0)));
    }

    bool IsTrimmed() const
    {
        return ReadConfig->TrimAtCall && ReadConfig->ReadCallCount >= ReadConfig->TrimAtCall->first;
    }

    void DoReportPersistedOffset(TOffset offsetExclusive) override
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{.CommittedOffsetExclusive = offsetExclusive});
    }

    void DoInit() final
    {
        ReadErrorState_ = CreateAvailabilityErrorState("/read");
        ExtraErrorState_ = CreateAvailabilityErrorState("/extra");
    }

private:
    const TTableSchemaPtr Schema_;
    std::optional<TBacklogRate> BacklogRate_;
    IStatusErrorStatePtr ReadErrorState_;
    IStatusErrorStatePtr ExtraErrorState_;
    i64 MaxOffsetExclusive_ = 0;
    TError Error_;
    std::vector<ui64> WriteTimestamps_;
    std::optional<i64> ProgressOffsetExclusive_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestSource);

class TTestSource
    : public TTestSourceImpl
{
public:
    static constexpr bool ReplayStable = true;

    using TTestSourceImpl::TTestSourceImpl;
};

DEFINE_REFCOUNTED_TYPE(TTestSource);

YT_FLOW_DEFINE_SOURCE(TTestSource);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TUndeclaredTestSource);

class TUndeclaredTestSource
    : public TTestSourceImpl
{
public:
    using TTestSourceImpl::TTestSourceImpl;
};

DEFINE_REFCOUNTED_TYPE(TUndeclaredTestSource);

YT_FLOW_DEFINE_SOURCE(TUndeclaredTestSource);

static_assert(TSourceIsReplayStable<TTestSource>::value);
static_assert(!TSourceIsReplayStable<TUndeclaredTestSource>::value);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCanonicalizingTestSource);

class TCanonicalizingTestSource
    : public TTestSource
{
public:
    using TTestSource::TTestSource;

    const std::optional<TOffset>& GetReportedPersistedOffset() const
    {
        return ReportedPersistedOffset_;
    }

private:
    TOffset NormalizeOffset(const TOffset& offset) const override
    {
        if (offset.Underlying().GetCount() == 0) {
            return offset;
        }
        YT_VERIFY(offset.Underlying().GetCount() == 2 || offset.Underlying().GetCount() == 4);
        return MakeKey(
            FromUnversionedValue<i64>(offset.Underlying()[0]),
            FromUnversionedValue<i64>(offset.Underlying()[1]));
    }

    void DoReportPersistedOffset(TOffset offsetExclusive) override
    {
        ReportedPersistedOffset_ = std::move(offsetExclusive);
    }

private:
    std::optional<TOffset> ReportedPersistedOffset_;
};

DEFINE_REFCOUNTED_TYPE(TCanonicalizingTestSource);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTestTimeProvider);

struct TTestTimeProvider
    : public ITimeProvider
{
    mutable std::atomic<TUniqueSeqNo::TUnderlying> CurrentSeqNo = 1ULL << 63;

    TTestTimeProvider() = default;

    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override
    {
        return MakeFuture(TGlobalUniqueSeqNo{
            .Timestamp = TSystemTimestamp(TInstant::Now().Seconds()),
            .UniqueSeqNo = TUniqueSeqNo{CurrentSeqNo.fetch_add(1)},
        });
    }

    i64 GenerateSeqNo() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> InsertSeqNoBarrier() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TSystemTimestamp> GetTimestamp(bool /*barrier*/) const override
    {
        YT_UNIMPLEMENTED();
    }
};

DEFINE_REFCOUNTED_TYPE(TTestTimeProvider);

////////////////////////////////////////////////////////////////////////////////

class TOrderedSourceTest
    : public ::testing::Test
{
public:
    TActionQueuePtr ActionQueue;
    TSourceContextPtr SourceContext;
    TSourceSpecPtr SourceSpec;
    TDynamicSourceSpecPtr DynamicSourceSpec;
    NYTree::IMapNodePtr DynamicSourcePartitionSpec;
    TMessageBatcherSettingsPtr DefaultBatcherSettings;
    TTestSourcePtr Source;
    TStateManagerMockPtr StateManager;

public:
    TDynamicSourceContextPtr MakeDynamicSourceContext()
    {
        auto ctx = New<TDynamicSourceContext>();
        ctx->DynamicSourceSpec = DynamicSourceSpec;
        ctx->DynamicPartitionSpec = DynamicSourcePartitionSpec;
        return ctx;
    }

    TTestSourcePtr MakeTestSource(
        const TSourceSpecPtr& spec,
        std::optional<i64> initialMaxOffset = 0,
        i64 initialCommittedOffset = 0)
    {
        SourceContext->SourceSpec = spec;
        return New<TTestSource>(SourceContext, MakeDynamicSourceContext(), initialMaxOffset, initialCommittedOffset);
    }

    template <typename TFunctor>
    auto RunInInvoker(TFunctor&& f)
    {
        auto errorOrValue = WaitFor(BIND(f).AsyncVia(ActionQueue->GetInvoker()).Run());
        if constexpr (requires { errorOrValue.ValueOrThrow(); }) {
            return errorOrValue.ValueOrThrow();
        } else {
            errorOrValue.ThrowOnError();
        }
    }

    void ReadAndPersistBatch(const TTestSourcePtr& source, const TMessageBatcherSettingsPtr& batcherSettings)
    {
        YT_VERIFY(GetCurrentInvoker() == ActionQueue->GetInvoker());
        const auto data = UnpackBatches(WaitFor(source->GetNextBatch(batcherSettings)).ValueOrThrow());
        for (const auto& item : data) {
            source->MarkPublished(item.Cookie);
            source->MarkPersisted(item.Cookie);
        }
        source->Sync();
        StateManager->Sync();
        source->Commit();
    }

    void SetUp() override
    {
        ActionQueue = New<TActionQueue>();

        SourceContext = CreateTestSourceContext(ActionQueue->GetInvoker());
        SourceContext->TimeProvider = New<TTestTimeProvider>();

        SourceSpec = ConvertTo<TSourceSpecPtr>(NYson::TYsonString(TStringBuf(R"""({
            "stream_id" = "test_input";
            "parameters" = {
                "finite" = %false;
            };
        })""")));
        SourceSpec->SourceClassName = TypeName<TTestSource>();

        DefaultBatcherSettings = New<TMessageBatcherSettings>();
        DefaultBatcherSettings->MaxRowsPerBatch = TSize(10);

        DynamicSourceSpec = New<TDynamicSourceSpec>();
        DynamicSourceSpec->Draining = false;
        DynamicSourceSpec->Parameters = NYTree::ConvertTo<IMapNodePtr>(
            ConvertTo<TOrderedSourceBase::TDynamicParametersPtr>(NYson::TYsonString(TStringBuf(R"""({
                "unavailable_threshold" = 500;
            })"""))));
        DynamicSourcePartitionSpec = GetEphemeralNodeFactory()->CreateMap();
        SourceContext->SourceSpec = SourceSpec;
        {
            auto dynamicSourceContext = New<TDynamicSourceContext>();
            dynamicSourceContext->DynamicSourceSpec = DynamicSourceSpec;
            dynamicSourceContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
            Source = New<TTestSource>(SourceContext, std::move(dynamicSourceContext));
        }
        StateManager = New<TStateManagerMock>();

        RunInInvoker([&] () {
            Source->Init(StateManager->CreateContext()->WithPrefix("source"));
        });
    }

    void Reset()
    {
        RunInInvoker([&] () {
            Source->Terminate();
        });
        ActionQueue->Shutdown();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedSourceTest, Simple)
{
    // Read from empty.
    {
        const auto data = RunInInvoker([&] () {
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 0u);
    }
    // Read from non-empty, check states.
    {
        const auto data = RunInInvoker([&] () {
            Source->SetMaxOffset(1);
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 1u);

        const auto publishedState = RunInInvoker([&] () {
            Source->MarkPublished(data[0].Cookie);
            Source->Sync();
            StateManager->Sync();
            return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        });
        ASSERT_EQ(OffsetToInt(publishedState->CommittedOffsetExclusive), 0LL);
        ASSERT_EQ(OffsetToInt(publishedState->PersistedOffsetExclusive), 0LL);
        ASSERT_EQ(OffsetToInt(publishedState->PublishedOffsetExclusive), 1LL);

        const auto persistedState = RunInInvoker([&] () {
            Source->MarkPersisted(data[0].Cookie);
            Source->Sync();
            StateManager->Sync();
            return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        });
        ASSERT_EQ(OffsetToInt(persistedState->CommittedOffsetExclusive), 0LL);
        ASSERT_EQ(OffsetToInt(persistedState->PersistedOffsetExclusive), 1LL);
        ASSERT_EQ(OffsetToInt(persistedState->PublishedOffsetExclusive), 1LL);

        const auto committedState = RunInInvoker([&] () {
            Source->Commit();
            Source->Sync();
            StateManager->Sync();
            return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        });
        ASSERT_EQ(OffsetToInt(committedState->CommittedOffsetExclusive), 1LL);
    }
    // Mark persisted out of order.
    {
        const auto data = RunInInvoker([&] () {
            Source->SetMaxOffset(3);
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 2u);

        const auto publishedState = RunInInvoker([&] () {
            Source->MarkPublished(data[1].Cookie);
            Source->MarkPersisted(data[1].Cookie);
            Source->Sync();
            StateManager->Sync();
            return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        });
        ASSERT_EQ(OffsetToInt(publishedState->PersistedOffsetExclusive), 1LL);
        ASSERT_EQ(OffsetToInt(publishedState->PublishedOffsetExclusive), 3LL);

        const auto persistedState = RunInInvoker([&] () {
            Source->MarkPublished(data[0].Cookie);
            Source->MarkPersisted(data[0].Cookie);
            Source->Sync();
            StateManager->Sync();
            return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        });
        ASSERT_EQ(OffsetToInt(persistedState->PersistedOffsetExclusive), 3LL);
        ASSERT_EQ(OffsetToInt(persistedState->PublishedOffsetExclusive), 3LL);
    }
}

TEST_F(TOrderedSourceTest, LegacyBoundariesAreNormalizedDuringInit)
{
    RunInInvoker([&] {
        Source->Terminate();
    });
    Source.Reset();

    const auto legacyBoundary = MakeKey(10, 2, -1, -1);
    auto legacyState = New<TOrderedSourcePartitionState>();
    legacyState->CommittedOffsetExclusive = legacyBoundary;
    legacyState->PersistedOffsetExclusive = legacyBoundary;
    legacyState->PublishedOffsetExclusive = legacyBoundary;
    legacyState->MaxOffsetExclusive = legacyBoundary;
    legacyState->OffsetMemory->clear();
    StateManager->Set("/source/v0", ConvertToYsonString(legacyState));

    const auto canonicalizingSource = New<TCanonicalizingTestSource>(SourceContext, MakeDynamicSourceContext());
    Source = canonicalizingSource;
    const auto [state, reportedPersistedOffset] = RunInInvoker([&] {
        canonicalizingSource->Init(StateManager->CreateContext()->WithPrefix("source"));
        StateManager->Sync();
        return std::pair(
            ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0")),
            canonicalizingSource->GetReportedPersistedOffset());
    });

    const auto canonicalBoundary = MakeKey(10, 2);
    EXPECT_EQ(state->CommittedOffsetExclusive, canonicalBoundary);
    EXPECT_EQ(state->PersistedOffsetExclusive, canonicalBoundary);
    EXPECT_EQ(state->PublishedOffsetExclusive, canonicalBoundary);
    EXPECT_EQ(state->MaxOffsetExclusive, canonicalBoundary);
    ASSERT_TRUE(reportedPersistedOffset);
    EXPECT_EQ(*reportedPersistedOffset, canonicalBoundary);

    Reset();
}

TEST_F(TOrderedSourceTest, ObsoleteOffsetsAreDropped)
{
    RunInInvoker([&] {
        Source->Terminate();
    });

    auto state = New<TOrderedSourcePartitionState>();
    state->CommittedOffsetExclusive = IntToOffset(7);
    state->PersistedOffsetExclusive = IntToOffset(7);
    state->PublishedOffsetExclusive = IntToOffset(7);
    state->MaxOffsetExclusive = IntToOffset(9);
    auto stateNode = ConvertToNode(state)->AsMap();
    for (const auto& key : {
            "committed_offset_exclusive",
            "persisted_offset_exclusive",
            "published_offset_exclusive",
            "max_offset_exclusive",
         })
    {
        stateNode->AddChild(key, ConvertToNode(3));
    }
    stateNode->AddChild("offset_memory", ConvertToNode(std::vector<std::pair<i64, ui64>>{{3, 1ULL << 63}}));
    StateManager->Set("/source/v0", ConvertToYsonString(stateNode));

    Source = MakeTestSource(SourceSpec, 9);
    const auto data = RunInInvoker([&] {
        Source->Init(StateManager->CreateContext()->WithPrefix("source"));
        auto data = UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        Source->Sync();
        StateManager->Sync();
        return data;
    });
    EXPECT_EQ(data.size(), 2u);

    const auto savedStateNode = ConvertToNode(StateManager->Get("/source/v0"))->AsMap();
    for (const auto& key : {
            "committed_offset_exclusive",
            "persisted_offset_exclusive",
            "published_offset_exclusive",
            "max_offset_exclusive",
            "offset_memory",
         })
    {
        EXPECT_FALSE(savedStateNode->FindChild(key)) << key;
    }
    EXPECT_EQ(OffsetToInt(ConvertTo<TOrderedSourcePartitionStatePtr>(savedStateNode)->PersistedOffsetExclusive), 7);

    Reset();
}

TEST_F(TOrderedSourceTest, PayloadlessRecordAdvancesWithoutMessages)
{
    const auto initialMessageId = RunInInvoker([&] {
        return Source->GetMaxPersistedMessageIdExclusive();
    });

    const auto [state, alignmentTimestamp] = RunInInvoker([&] {
        Source->SetProgressRecord(5);
        auto batches = WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow();
        EXPECT_TRUE(batches.empty());
        auto alignmentTimestamp = Source->GetReadAlignmentTimestamp();
        Source->Sync();
        StateManager->Sync();
        return std::pair(
            ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0")),
            alignmentTimestamp);
    });

    EXPECT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(state->PublishedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(state->MaxOffsetExclusive), 5);
    EXPECT_TRUE(state->OffsetMemory->empty());
    EXPECT_TRUE(state->AlignmentTimestampMemory->empty());
    EXPECT_EQ(state->LastPersistedWriteTimestamp, TSystemTimestamp(5));
    EXPECT_EQ(alignmentTimestamp, TSystemTimestamp(5));
    EXPECT_GT(state->PersistedMessageIdExclusive, initialMessageId);
}

TEST_F(TOrderedSourceTest, PayloadlessRecordWaitsForInflightRecord)
{
    const auto data = RunInInvoker([&] {
        Source->SetMaxOffset(1);
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 1u);

    const auto blockedState = RunInInvoker([&] {
        Source->SetProgressRecord(5);
        auto batches = WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow();
        EXPECT_TRUE(batches.empty());
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });
    EXPECT_EQ(OffsetToInt(blockedState->PersistedOffsetExclusive), 0);
    EXPECT_EQ(OffsetToInt(blockedState->PublishedOffsetExclusive), 0);
    EXPECT_EQ(OffsetToInt(blockedState->MaxOffsetExclusive), 5);

    const auto advancedState = RunInInvoker([&] {
        Source->MarkPublished(data[0].Cookie);
        Source->MarkPersisted(data[0].Cookie);
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });
    EXPECT_EQ(OffsetToInt(advancedState->PersistedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(advancedState->PublishedOffsetExclusive), 5);
    EXPECT_TRUE(advancedState->OffsetMemory->empty());
    EXPECT_EQ(advancedState->LastPersistedWriteTimestamp, TSystemTimestamp(5));
}

TEST_F(TOrderedSourceTest, PayloadRecordsWithProgressTailAdvanceToContinuation)
{
    const auto data = RunInInvoker([&] {
        Source->SetMaxOffset(2);
        Source->SetProgressRecord(5);
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 2u);

    const auto state = RunInInvoker([&] {
        for (const auto& item : data) {
            Source->MarkPublished(item.Cookie);
            Source->MarkPersisted(item.Cookie);
        }
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });

    EXPECT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(state->PublishedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(state->MaxOffsetExclusive), 5);
    EXPECT_TRUE(state->OffsetMemory->empty());
    EXPECT_EQ(state->LastPersistedWriteTimestamp, TSystemTimestamp(5));
}

TEST_F(TOrderedSourceTest, ReplayAfterCheckpointPreservesMessageId)
{
    TMessageId originalMessageId;
    {
        const auto data = RunInInvoker([&] {
            Source->SetMaxOffset(1);
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 1u);
        originalMessageId = data[0].Message->MessageId;

        RunInInvoker([&] {
            Source->MarkPublished(data[0].Cookie);
            Source->Sync();
            StateManager->Sync();
        });
    }

    RunInInvoker([&] {
        Source->Terminate();
    });
    Source.Reset();

    Source = MakeTestSource(SourceSpec);
    {
        const auto replayedData = RunInInvoker([&] {
            Source->Init(StateManager->CreateContext()->WithPrefix("source"));
            Source->SetMaxOffset(1);
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(replayedData.size(), 1u);
        EXPECT_EQ(replayedData[0].Message->MessageId, originalMessageId);

        RunInInvoker([&] {
            Source->MarkPublished(replayedData[0].Cookie);
            Source->MarkPersisted(replayedData[0].Cookie);
            Source->Sync();
            StateManager->Sync();
        });
    }

    Reset();
}

TEST_F(TOrderedSourceTest, SourceTotalCounters)
{
    const auto advanceMaxOffset = [&] (i64 maxOffset) {
        return RunInInvoker([&] () {
            Source->SetMaxOffset(maxOffset);
            Y_UNUSED(Source->BuildInflight());
            return std::pair(Source->GetSourceTotalCount(), Source->GetSourceTotalBytes());
        });
    };

    const auto [count, bytes] = advanceMaxOffset(2);
    EXPECT_DOUBLE_EQ(count, 2.0);
    EXPECT_DOUBLE_EQ(bytes, 2.0);

    const auto [increasedCount, increasedBytes] = advanceMaxOffset(5);
    EXPECT_DOUBLE_EQ(increasedCount, 5.0);
    EXPECT_DOUBLE_EQ(increasedBytes, 5.0);

    const auto [unchangedCount, unchangedBytes] = advanceMaxOffset(5);
    EXPECT_DOUBLE_EQ(unchangedCount, 5.0);
    EXPECT_DOUBLE_EQ(unchangedBytes, 5.0);
}

TEST_F(TOrderedSourceTest, BacklogRateContributesToNewRate)
{
    const auto inflight = RunInInvoker([&] {
        Source->SetBacklogRate(TBacklogRate{
            .BytesPerSecond = 456,
            .MessagesPerSecond = 123,
        });
        return Source->BuildInflight();
    });
    EXPECT_DOUBLE_EQ(*inflight->InflightMetrics->NewCountPerSec, 123);
    EXPECT_DOUBLE_EQ(*inflight->InflightMetrics->NewBytesPerSec, 456);
    EXPECT_DOUBLE_EQ(*inflight->InflightMetrics->OfferedCountPerSec, 123);
    EXPECT_DOUBLE_EQ(*inflight->InflightMetrics->OfferedBytesPerSec, 456);
}

TEST_F(TOrderedSourceTest, ReadyTracksUnreadPartOfExternalBacklog)
{
    auto beforeRead = RunInInvoker([&] {
        Source->SetMaxOffset(5);
        return Source->BuildInflight();
    });
    ASSERT_EQ(beforeRead->InflightMetrics->Count, 5);
    ASSERT_EQ(beforeRead->InflightMetrics->ReadyCount, 5);
    EXPECT_EQ(Source->GetSourceTotalCount(), 5);

    const auto messages = RunInInvoker([&] {
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(messages.size(), 5u);
    auto afterRead = RunInInvoker([&] {
        return Source->BuildInflight();
    });
    EXPECT_EQ(afterRead->InflightMetrics->Count, 5);
    EXPECT_EQ(afterRead->InflightMetrics->ReadyCount, 0);
    EXPECT_EQ(Source->GetSourceTotalCount(), 5);

    const auto empty = RunInInvoker([&] {
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ(Source->GetSourceTotalCount(), 5);
}

TEST_F(TOrderedSourceTest, EmptyPartition)
{
    for (i64 maxOffset : {0, 1}) {
        // Check empty.
        {
            const auto inflight = RunInInvoker([&] () {
                return Source->BuildInflight();
            });
            ASSERT_FALSE(inflight->Suspended);
            ASSERT_FALSE(inflight->Empty);
            ASSERT_EQ(inflight->InflightMetrics->Count, 0LL);
            ASSERT_FALSE(inflight->InflightMetrics->IdleDuration)
                << "Actual value: " << ToString(*inflight->InflightMetrics->IdleDuration);
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
        }
        // Check that IdleDuration > 0.
        {
            const auto inflight = RunInInvoker([&] () {
                Source->SetMaxOffset(maxOffset);
                TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                Source->SetMaxOffset(maxOffset);
                return Source->BuildInflight();
            });
            ASSERT_EQ(inflight->InflightMetrics->Count, 0LL);
            ASSERT_TRUE(inflight->InflightMetrics->IdleDuration);
            ASSERT_GE(inflight->InflightMetrics->IdleDuration, TDuration::Seconds(1));
        }

        RunInInvoker([&] () {
            Source->SetMaxOffset(maxOffset + 1);
            ReadAndPersistBatch(Source, DefaultBatcherSettings);
        });
    }

    // Check outdated update of MaxOffset.
    {
        const auto inflight = RunInInvoker([&] () {
            Y_UNUSED(Source->BuildInflight()); // Flush update with committed offset.
            auto now = TInstant::Now();
            Source->SetMaxOffset(2, now + TDuration::Seconds(10));
            Source->SetMaxOffset(2, now - TDuration::Seconds(100)); // Outdated update.
            return Source->BuildInflight();
        });
        ASSERT_GE(inflight->InflightMetrics->IdleDuration, TDuration::Seconds(10));
    }
}

TEST_F(TOrderedSourceTest, Completed)
{
    auto spec = ConvertTo<TSourceSpecPtr>(NYson::TYsonString(TStringBuf(R"""({
        "stream_id" = "test_input";
        "parameters" = {
            "finite" = %true;
        };
    })""")));
    spec->SourceClassName = TypeName<TTestSource>();

    for (const i64 maxOffset : {0, 1}) {
        auto finiteSource = MakeTestSource(spec);

        const auto inflight = RunInInvoker([&] () {
            finiteSource->Init(StateManager->CreateContext("finite_source"));
            finiteSource->SetMaxOffset(maxOffset);
            ReadAndPersistBatch(finiteSource, DefaultBatcherSettings);
            return finiteSource->BuildInflight();
        });

        ASSERT_TRUE(inflight->Empty);

        RunInInvoker([&] () {
            finiteSource->Terminate();
        });
    }
}

TEST_F(TOrderedSourceTest, UnavailablePartition)
{
    const auto readAndAssertEmpty = [&] () {
        const auto data = RunInInvoker([&] () {
            return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 0u);
        Y_UNUSED(data);
    };
    for (i64 maxOffset : {0, 1}) {
        auto description = Format("MaxOffset: %v, DynamicConfig: %v", maxOffset, ConvertToYsonString(DynamicSourceSpec, EYsonFormat::Text));

        RunInInvoker([&] () {
            return Source->SetTestError(TError("test error"));
        });

        // Check unavailable.
        {
            readAndAssertEmpty();
            const auto inflight = RunInInvoker([&] () {
                return Source->BuildInflight();
            });
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp) << description; // Just one unavailability event.
        }
        // Check that unavailable for some time.
        {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            readAndAssertEmpty();
            const auto inflight = RunInInvoker([&] () {
                return Source->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp) << description;
            ASSERT_EQ(SourceContext->StatusProfiler->GetStatus().Errors.size(), 1u) << description;
        }

        RunInInvoker([&] () {
            Source->SetTestError(TError());
            Source->SetMaxOffset(maxOffset + 1);
            ReadAndPersistBatch(Source, DefaultBatcherSettings);
        });
    }
}

TEST_F(TOrderedSourceTest, UnavailablePartitionAfterRestart)
{
    const auto readAndAssertEmpty = [&] (TTestSourcePtr source) {
        const auto data = RunInInvoker([&] () {
            return UnpackBatches(WaitFor(source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 0u);
        Y_UNUSED(data);
    };
    {
        const auto oldSource = MakeTestSource(SourceSpec);
        RunInInvoker([&] () {
            oldSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });

        RunInInvoker([&] () {
            return oldSource->SetTestError(TError("test error"));
        });
        {
            readAndAssertEmpty(oldSource);
            const auto inflight = RunInInvoker([&] () {
                return oldSource->BuildInflight();
            });
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            readAndAssertEmpty(oldSource);
            const auto inflight = RunInInvoker([&] () {
                return oldSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }
        RunInInvoker([&] () {
            oldSource->Sync();
            StateManager->Sync();
        });
    }
    {
        const auto newSource = MakeTestSource(SourceSpec);
        RunInInvoker([&] () {
            newSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });
        {
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        RunInInvoker([&] () {
            return newSource->SetTestError(TError("test error"));
        });
        {
            readAndAssertEmpty(newSource);
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }
        {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            readAndAssertEmpty(newSource);
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }
    }
}

TEST_F(TOrderedSourceTest, RepositionedUpdateSkipsForward)
{
    // An external position ahead of the persisted offset (a consumer offset an operator moved) is
    // honored like a trim rewind, minus the alarm.
    const auto source = MakeTestSource(SourceSpec);
    RunInInvoker([&] () {
        source->Init(StateManager->CreateContext()->WithPrefix("reposition"));
        source->Reposition(100);
    });

    const auto data = RunInInvoker([&] () {
        source->SetMaxOffset(105);
        return UnpackBatches(WaitFor(source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 5u);
    ASSERT_EQ(GetColumnValue<i64>(data[0].Message, "data"), 100);

    const auto state = RunInInvoker([&] () {
        source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/reposition/v0"));
    });
    ASSERT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 100LL);
}

TEST_F(TOrderedSourceTest, RestartGapIsNotCountedAsUnavailable)
{
    const auto readAndAssertEmpty = [&] (const TTestSourcePtr& source) {
        const auto data = RunInInvoker([&] () {
            return UnpackBatches(WaitFor(source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 0u);
    };

    // Accumulate a little under the 500 ms threshold, then persist.
    {
        const auto oldSource = MakeTestSource(SourceSpec);
        RunInInvoker([&] () {
            oldSource->Init(StateManager->CreateContext()->WithPrefix("gap"));
            oldSource->SetTestError(TError("test error"));
        });
        readAndAssertEmpty(oldSource);
        RunInInvoker([&] () {
            return oldSource->BuildInflight();
        });

        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
        readAndAssertEmpty(oldSource);
        const auto inflight = RunInInvoker([&] () {
            return oldSource->BuildInflight();
        });
        ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);

        RunInInvoker([&] () {
            oldSource->Sync();
            StateManager->Sync();
        });
    }

    const auto newSource = MakeTestSource(SourceSpec);
    RunInInvoker([&] () {
        newSource->Init(StateManager->CreateContext()->WithPrefix("gap"));
    });

    // Nobody was watching for this second, so it is charged to nobody: the accumulator resumes at the
    // ~300 ms it stopped at rather than jumping past the threshold.
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    RunInInvoker([&] () {
        newSource->SetTestError(TError("test error"));
    });
    readAndAssertEmpty(newSource);
    {
        const auto inflight = RunInInvoker([&] () {
            return newSource->BuildInflight();
        });
        ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
    }

    // Another 300 ms of observed failure does cross it.
    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(300));
    readAndAssertEmpty(newSource);
    {
        const auto inflight = RunInInvoker([&] () {
            return newSource->BuildInflight();
        });
        ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
    }
}

TEST_F(TOrderedSourceTest, ExtraAvailabilityErrorStateOutvotesHealthyRead)
{
    RunInInvoker([&] () {
        Source->SetMaxOffset(1);
        ReadAndPersistBatch(Source, DefaultBatcherSettings);
        Source->SetExtraError(TError("liveness error"));
    });

    // The read state is OK and stays OK; the extra state alone must carry the partition to unavailable.
    RunInInvoker([&] () {
        return Source->BuildInflight();
    });
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    {
        const auto inflight = RunInInvoker([&] () {
            return Source->BuildInflight();
        });
        ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
    }

    RunInInvoker([&] () {
        Source->SetExtraError(TError());
    });
    {
        const auto inflight = RunInInvoker([&] () {
            return Source->BuildInflight();
        });
        ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
    }
}

TEST_F(TOrderedSourceTest, HealthyStateDoesNotVouchForABrokenOne)
{
    RunInInvoker([&] () {
        Source->SetMaxOffset(1);
        Source->SetExtraError(TError("liveness error"));
    });

    // /read keeps re-affirming OK on every read while /extra stays broken. A source that cannot deliver
    // must still latch: an OK from one state may not end a failure run another state is still reporting.
    for (int iteration = 0; iteration < 4; ++iteration) {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        RunInInvoker([&] () {
            ReadAndPersistBatch(Source, DefaultBatcherSettings);
            return Source->BuildInflight();
        });
    }

    const auto inflight = RunInInvoker([&] () {
        return Source->BuildInflight();
    });
    ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
}

TEST_F(TOrderedSourceTest, UnavailableGroupSilencesErrorsButNotAccounting)
{
    RunInInvoker([&] () {
        Source->SetTestError(TError("test error"));
        Source->SetExtraError(TError("liveness error"));
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(SourceContext->StatusProfiler->GetStatus().Errors.size(), 2u);

    RunInInvoker([&] () {
        auto dynamicSourceContext = MakeDynamicSourceContext();
        dynamicSourceContext->AvailabilityGroupUnavailable = true;
        Source->Reconfigure(dynamicSourceContext);
    });
    ASSERT_TRUE(SourceContext->StatusProfiler->GetStatus().Errors.empty());

    // Silencing must not reach the accounting: the verdict that silenced these errors is derived from
    // them, so hiding them from the tree may not hide them from BuildInflight.
    TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    RunInInvoker([&] () {
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    {
        const auto inflight = RunInInvoker([&] () {
            return Source->BuildInflight();
        });
        ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
    }

    RunInInvoker([&] () {
        auto dynamicSourceContext = MakeDynamicSourceContext();
        dynamicSourceContext->AvailabilityGroupUnavailable = false;
        Source->Reconfigure(dynamicSourceContext);
    });
    ASSERT_EQ(SourceContext->StatusProfiler->GetStatus().Errors.size(), 2u);
}

TEST_F(TOrderedSourceTest, UnavailablePartitionTurnsAvailableAfterRestart)
{
    const auto readAndAssertEmpty = [&] (TTestSourcePtr source) {
        const auto data = RunInInvoker([&] () {
            return UnpackBatches(WaitFor(source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        });
        ASSERT_EQ(data.size(), 0u);
        Y_UNUSED(data);
    };
    {
        const auto oldSource = MakeTestSource(SourceSpec);
        RunInInvoker([&] () {
            oldSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });

        RunInInvoker([&] () {
            return oldSource->SetTestError(TError("test error"));
        });
        {
            readAndAssertEmpty(oldSource);
            const auto inflight = RunInInvoker([&] () {
                return oldSource->BuildInflight();
            });
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            readAndAssertEmpty(oldSource);
            const auto inflight = RunInInvoker([&] () {
                return oldSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }
        RunInInvoker([&] () {
            oldSource->Sync();
            StateManager->Sync();
        });
    }
    {
        const auto newSource = MakeTestSource(SourceSpec);
        RunInInvoker([&] () {
            newSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });

        {
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        RunInInvoker([&] () {
            newSource->SetTestError(TError());
            newSource->SetMaxOffset(1);
            ReadAndPersistBatch(newSource, DefaultBatcherSettings);
        });
        {
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        RunInInvoker([&] () {
            return newSource->SetTestError(TError("test error"));
        });
        {
            readAndAssertEmpty(newSource);
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_FALSE(inflight->InflightMetrics->UnavailableTimestamp);
        }

        {
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            readAndAssertEmpty(newSource);
            const auto inflight = RunInInvoker([&] () {
                return newSource->BuildInflight();
            });
            ASSERT_TRUE(inflight->InflightMetrics->UnavailableTimestamp);
        }
    }
}

TEST_F(TOrderedSourceTest, Draining)
{
    {
        RunInInvoker([&] () {
            auto dynamicSourceSpec = CloneYsonStruct(DynamicSourceSpec);
            dynamicSourceSpec->Draining = true;
            auto dynamicSourceContext = New<TDynamicSourceContext>();
            dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
            dynamicSourceContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
            Source->Reconfigure(dynamicSourceContext);
        });

        // Drain from empty.
        {
            const auto data = RunInInvoker([&] () {
                return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
            });
            ASSERT_EQ(data.size(), 0u);
        }
        // Drain from non-empty.
        {
            const auto emptyData = RunInInvoker([&] () {
                Source->SetMaxOffset(7);
                return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
            });
            ASSERT_EQ(emptyData.size(), 0u);

            const auto data = RunInInvoker([&] () {
                auto dynamicSourceSpec = CloneYsonStruct(DynamicSourceSpec);
                dynamicSourceSpec->Draining = false;
                auto dynamicSourceContext = New<TDynamicSourceContext>();
                dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
                dynamicSourceContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
                Source->Reconfigure(dynamicSourceContext);
                return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
            });

            ASSERT_EQ(data.size(), 7u);

            auto stateForDraining = RunInInvoker([&] () {
                for (const auto& item : data) {
                    Source->MarkPublished(item.Cookie);
                }
                Source->Sync();
                StateManager->Sync();
                return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
            });

            RunInInvoker([&] () {
                for (const auto& item : data) {
                    Source->MarkPersisted(item.Cookie);
                }
            });
        }
    }
    // Recover from state and drain.
    {
        auto newSourceDynamicContext = New<TDynamicSourceContext>();
        newSourceDynamicContext->DynamicSourceSpec = DynamicSourceSpec;
        newSourceDynamicContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
        const auto newSource = New<TTestSource>(SourceContext, std::move(newSourceDynamicContext));
        const auto stateForDraining = ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
        ASSERT_EQ(OffsetToInt(stateForDraining->PersistedOffsetExclusive), 0LL);
        ASSERT_EQ(OffsetToInt(stateForDraining->PublishedOffsetExclusive), 7LL);
        StateManager->Set("/new_source/v0", ConvertToYsonString(stateForDraining));

        const auto data = RunInInvoker([&] () {
            auto dynamicSourceSpec = CloneYsonStruct(DynamicSourceSpec);
            dynamicSourceSpec->Draining = true;
            auto dynamicSourceContext = New<TDynamicSourceContext>();
            dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
            dynamicSourceContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
            newSource->Reconfigure(dynamicSourceContext);
            newSource->Init(StateManager->CreateContext()->WithPrefix("new_source"));
            newSource->SetMaxOffset(1000);
            return WaitFor(newSource->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow();
        });

        ASSERT_EQ(data.size(), 7u);

        RunInInvoker([&] () {
            newSource->Terminate();
        });
    }
}

TEST_F(TOrderedSourceTest, CommittedBeforePersistAtStart)
{
    // No crash is OK.
    const auto data = RunInInvoker([&] () {
        // Initial committed offset is greater than zero.
        Source->UpdatePartitionInfo(TTestSource::TPartitionInfoUpdate{.CommittedOffsetExclusive = IntToOffset(10), .MaxOffsetExclusive = IntToOffset(15)});
        Source->Sync();
        StateManager->Sync();
        Y_UNUSED(Source->BuildInflight());
        Source->SetMaxOffset(20);

        const auto data = UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
        for (const auto& item : data) {
            Source->MarkPublished(item.Cookie);
            Source->MarkPersisted(item.Cookie);
        }
        Source->Sync();
        StateManager->Sync();
        Source->Commit();

        Y_UNUSED(Source->BuildInflight());
        return data;
    });
    ASSERT_EQ(data.size(), 10u);
}

DEFINE_ENUM(ETestUnexpectedTrimPosition,
    ((BeforePublishing)                  (1))
    ((BeforePersisting)                  (2))
    ((BeforeMarkStatePersisted)     (3))
    ((End)                               (4))
);

class TOrderedSourceUnexpectedTrimPositionTest
    : public TOrderedSourceTest
    , public ::testing::WithParamInterface<ETestUnexpectedTrimPosition>
{ };

TEST_P(TOrderedSourceUnexpectedTrimPositionTest, )
{
    auto onPosition = [&] (ETestUnexpectedTrimPosition position) {
        if (position == GetParam()) {
            Source->SetCommittedOffset(20);
            Source->Sync();
            StateManager->Sync();
        }
    };

    const auto data = RunInInvoker([&] () {
        Source->SetMaxOffset(100);
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 10u);

    const auto publishedState = RunInInvoker([&] () {
        onPosition(ETestUnexpectedTrimPosition::BeforePublishing);
        for (const auto& item : data) {
            Source->MarkPublished(item.Cookie);
        }
        onPosition(ETestUnexpectedTrimPosition::BeforePersisting);
        for (const auto& item : data) {
            Source->MarkPersisted(item.Cookie);
        }
        onPosition(ETestUnexpectedTrimPosition::BeforeMarkStatePersisted);
        Source->Commit();
        onPosition(ETestUnexpectedTrimPosition::End);
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });
    ASSERT_EQ(OffsetToInt(publishedState->CommittedOffsetExclusive), 20LL);
    ASSERT_EQ(OffsetToInt(publishedState->PersistedOffsetExclusive), 20LL);
    ASSERT_EQ(OffsetToInt(publishedState->PublishedOffsetExclusive), 20LL);
    ASSERT_EQ(GetColumnValue<i64>(data[0].Message, "data"), 0);

    const auto nextData = RunInInvoker([&] () {
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(GetColumnValue<i64>(nextData[0].Message, "data"), 20);
}

INSTANTIATE_TEST_SUITE_P(
    , TOrderedSourceUnexpectedTrimPositionTest,
    ::testing::ValuesIn(TEnumTraits<ETestUnexpectedTrimPosition>::GetDomainValues()),
    [] (const testing::TestParamInfo<ETestUnexpectedTrimPosition>& info) -> std::string {
        return ToString(info.param);
    });

TEST_F(TOrderedSourceTest, UnorderedUpdates)
{
    const auto data = RunInInvoker([&] () {
        Source->SetMaxOffset(30);
        Source->SetMaxOffset(100);
        Source->SetMaxOffset(10);
        Source->SetCommittedOffset(20);
        Source->SetCommittedOffset(95);
        Source->SetCommittedOffset(70);
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 5u);
}

TEST_F(TOrderedSourceTest, ArrivalRateExcludesInitialBacklogAndIgnoresLowerMaximum)
{
    struct TCase
    {
        std::string Name;
        i64 CommittedOffset;
        i64 RestoredMaximum;
        TTestSourcePtr Source;
        TInflightMetricsPtr Before;
    };

    std::vector<TCase> cases{
        {"fresh", 0, 0, {}, {}},
        {"trimmed", 50, 0, {}, {}},
        {"restored", 50, 70, {}, {}},
        {"restored_equal", 50, 100, {}, {}},
    };
    TTestSourcePtr unconfirmedSource;
    RunInInvoker([&] {
        auto spec = CloneYsonStruct(SourceSpec);
        spec->Parameters->AddChild("update_info_period", ConvertToNode(TDuration::Hours(1)));
        unconfirmedSource = MakeTestSource(spec, std::nullopt);
        unconfirmedSource->Init(StateManager->CreateContext()->WithPrefix("unconfirmed_source_rate"));
        for (auto& testCase : cases) {
            auto prefix = "source_rate_" + testCase.Name;
            if (testCase.RestoredMaximum) {
                auto state = New<TOrderedSourcePartitionState>();
                state->MaxOffsetExclusive = IntToOffset(testCase.RestoredMaximum);
                state->MaxOffsetIsConfirmed = true;
                StateManager->Set("/" + prefix + "/v0", ConvertToYsonString(state));
            }
            testCase.Source = MakeTestSource(spec, 100, testCase.CommittedOffset);
            testCase.Source->Init(StateManager->CreateContext()->WithPrefix(prefix));
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(1));
            testCase.Source->BuildInflight();
        }
    });

    // Warm up the real counters together without adding a test clock to the source API.
    RunInInvoker([&] {
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(31));
    });
    auto unconfirmed = RunInInvoker([&] {
        unconfirmedSource->SetCommittedOffset(10);
        return unconfirmedSource->BuildInflight()->InflightMetrics;
    });
    EXPECT_FALSE(unconfirmed->NewCountPerSec);
    EXPECT_FALSE(unconfirmed->NewBytesPerSec);
    EXPECT_FALSE(unconfirmed->OfferedCountPerSec);
    EXPECT_FALSE(unconfirmed->OfferedBytesPerSec);

    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        const auto& source = testCase.Source;
        auto initial = RunInInvoker([&] {
            source->SetMaxOffset(100);
            return source->BuildInflight()->InflightMetrics;
        });
        ASSERT_TRUE(initial->NewCountPerSec);
        ASSERT_TRUE(initial->NewBytesPerSec);
        EXPECT_DOUBLE_EQ(*initial->NewCountPerSec, 0);
        EXPECT_DOUBLE_EQ(*initial->NewBytesPerSec, 0);
        EXPECT_EQ(initial->OfferedCountPerSec, initial->NewCountPerSec);
        EXPECT_EQ(initial->OfferedBytesPerSec, initial->NewBytesPerSec);
        EXPECT_EQ(initial->ReadyCount, 100 - testCase.CommittedOffset);
        EXPECT_DOUBLE_EQ(source->GetSourceTotalCount(), 100 - testCase.RestoredMaximum);
        EXPECT_DOUBLE_EQ(source->GetSourceTotalBytes(), 100 - testCase.RestoredMaximum);
    }

    RunInInvoker([&] {
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    });
    for (auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        testCase.Before = RunInInvoker([&] {
            testCase.Source->SetMaxOffset(200);
            return testCase.Source->BuildInflight()->InflightMetrics;
        });
        ASSERT_TRUE(testCase.Before->NewCountPerSec);
        ASSERT_TRUE(testCase.Before->NewBytesPerSec);
        EXPECT_GT(*testCase.Before->NewCountPerSec, 0);
        EXPECT_GT(*testCase.Before->NewBytesPerSec, 0);
    }

    RunInInvoker([&] {
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    });
    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        auto afterLower = RunInInvoker([&] {
            testCase.Source->SetMaxOffset(10);
            return testCase.Source->BuildInflight()->InflightMetrics;
        });
        EXPECT_EQ(afterLower->NewCountPerSec, testCase.Before->NewCountPerSec);
        EXPECT_EQ(afterLower->NewBytesPerSec, testCase.Before->NewBytesPerSec);
        EXPECT_EQ(afterLower->OfferedCountPerSec, testCase.Before->NewCountPerSec);
        EXPECT_EQ(afterLower->OfferedBytesPerSec, testCase.Before->NewBytesPerSec);
    }

    RunInInvoker([&] {
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
    });
    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        auto afterUnchanged = RunInInvoker([&] {
            testCase.Source->SetMaxOffset(200);
            return testCase.Source->BuildInflight()->InflightMetrics;
        });
        ASSERT_TRUE(afterUnchanged->NewCountPerSec);
        ASSERT_TRUE(afterUnchanged->NewBytesPerSec);
        EXPECT_LT(*afterUnchanged->NewCountPerSec, *testCase.Before->NewCountPerSec);
        EXPECT_LT(*afterUnchanged->NewBytesPerSec, *testCase.Before->NewBytesPerSec);
    }
}

TEST_F(TOrderedSourceTest, Timestamps)
{
    const auto data = RunInInvoker([&] () {
        Source->SetMaxOffset(20);
        return UnpackBatches(WaitFor(Source->GetNextBatch(DefaultBatcherSettings)).ValueOrThrow());
    });
    ASSERT_EQ(data.size(), 10u);

    const auto publishedState = RunInInvoker([&] () {
        for (const auto& item : data) {
            Source->MarkPublished(item.Cookie);
        }
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });
    ASSERT_FALSE(publishedState->LastPersistedWriteTimestamp.has_value());
    ASSERT_EQ(publishedState->FirstNotPersistedWriteTimestamp, std::optional<TSystemTimestamp>(TSystemTimestamp(1LL)));

    const auto partiallyPersistedState = RunInInvoker([&] () {
        Source->MarkPersisted(data[0].Cookie);
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });
    ASSERT_EQ(partiallyPersistedState->LastPersistedWriteTimestamp, std::optional<TSystemTimestamp>(TSystemTimestamp(1LL)));
    ASSERT_EQ(partiallyPersistedState->FirstNotPersistedWriteTimestamp, std::optional<TSystemTimestamp>(TSystemTimestamp(2LL)));

    const auto persistedState = RunInInvoker([&] () {
        for (size_t i = 1; i < data.size(); ++i) {
            Source->MarkPersisted(data[i].Cookie);
        }
        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });

    ASSERT_EQ(persistedState->LastPersistedWriteTimestamp, std::optional<TSystemTimestamp>(TSystemTimestamp(10LL)));
    ASSERT_FALSE(persistedState->FirstNotPersistedWriteTimestamp.has_value());

    const auto finalState = RunInInvoker([&] () {
        ReadAndPersistBatch(Source, DefaultBatcherSettings);

        Source->Sync();
        StateManager->Sync();
        Source->Commit();

        // Actualize max offset.
        Source->SetMaxOffset(20);

        Source->Sync();
        StateManager->Sync();
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    });

    ASSERT_FALSE(finalState->LastPersistedWriteTimestamp.has_value());
    ASSERT_FALSE(finalState->FirstNotPersistedWriteTimestamp.has_value());
}

TEST_F(TOrderedSourceTest, AlignmentTimestamp)
{
    // clang-format off
    const std::vector<ui64> writeTimestamps = {
        2, 1,
        3, 4,
        1, 1,
        10, 7,
        6, 6,
    };
    auto alignmentTimestampsChecker = ::testing::ElementsAre(
        1, 1,
        3, 3,
        3, 3,
        7, 7,
        7, 7
    );
    // clang-format on

    auto batcherSettings = CloneYsonStruct(DefaultBatcherSettings);
    batcherSettings->MaxRowsPerBatch = TSize(2);

    auto read = [&] (TTestSourcePtr source) {
        return RunInInvoker([&] () {
            source->SetMaxOffset(writeTimestamps.size());
            return UnpackBatches(WaitFor(source->GetNextBatch(batcherSettings)).ValueOrThrow());
        });
    };

    std::vector<ui64> actualAlignmentTimestamps;
    auto readAndMarkPersistedAndMemorize = [&] (TTestSourcePtr source) {
        auto data = read(source);
        RunInInvoker([&] () {
            for (const auto& item : data) {
                source->MarkPublished(item.Cookie);
                source->MarkPersisted(item.Cookie);
                actualAlignmentTimestamps.push_back(item.Message->AlignmentTimestamp.Underlying());
            }
        });
        return data;
    };

    {
        const auto oldSource = MakeTestSource(SourceSpec);
        oldSource->SetTestWriteTimestamps(writeTimestamps);
        RunInInvoker([&] () {
            oldSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });

        readAndMarkPersistedAndMemorize(oldSource);
        readAndMarkPersistedAndMemorize(oldSource);
        read(oldSource);

        RunInInvoker([&] () {
            oldSource->Sync();
            StateManager->Sync();
        });
    }
    {
        const auto newSource = MakeTestSource(SourceSpec);
        auto modifiedWriteTimestamps = writeTimestamps;
        modifiedWriteTimestamps[4] = 5; // These values must not be used due alignment timestamp memory.
        modifiedWriteTimestamps[5] = 5;
        newSource->SetTestWriteTimestamps(modifiedWriteTimestamps);
        RunInInvoker([&] () {
            newSource->Init(StateManager->CreateContext()->WithPrefix("restart"));
        });

        readAndMarkPersistedAndMemorize(newSource);
        readAndMarkPersistedAndMemorize(newSource);
        readAndMarkPersistedAndMemorize(newSource);
    }

    ASSERT_THAT(actualAlignmentTimestamps, alignmentTimestampsChecker);
}

////////////////////////////////////////////////////////////////////////////////

class TStableReadGroupsTest
    : public TOrderedSourceTest
{
public:
    std::shared_ptr<TTestReadConfig> ReadConfig = std::make_shared<TTestReadConfig>();

    void SetUp() override
    {
        TOrderedSourceTest::SetUp();
        Restart();
    }

    //! Recreates the source from the last synced state, like a job restart.
    void Restart(i64 maxOffset = 0, bool stable = true)
    {
        RunInInvoker([&] {
            Source->Terminate();
        });
        Source.Reset();
        Source = MakeTestSource(SourceSpec);
        Source->ReadConfig = ReadConfig;
        RunInInvoker([&] {
            if (stable) {
                Source->EnableStableReadGroups();
            }
            Source->Init(StateManager->CreateContext()->WithPrefix("source"));
            if (maxOffset > 0) {
                Source->SetMaxOffset(maxOffset);
            }
        });
    }

    std::vector<TUnpackedMessage> Read(std::optional<i64> maxRows = {})
    {
        auto settings = DefaultBatcherSettings;
        if (maxRows) {
            settings = New<TMessageBatcherSettings>();
            settings->MaxRowsPerBatch = TSize(*maxRows);
        }
        return RunInInvoker([&] {
            return UnpackBatches(WaitFor(Source->GetNextBatch(settings)).ValueOrThrow());
        });
    }

    void Checkpoint(const std::vector<TUnpackedMessage>& data, bool persist)
    {
        RunInInvoker([&] {
            for (const auto& item : data) {
                Source->MarkPublished(item.Cookie);
                if (persist) {
                    Source->MarkPersisted(item.Cookie);
                }
            }
            Source->Sync();
            StateManager->Sync();
            Source->Commit();
        });
    }

    TOrderedSourcePartitionStatePtr GetState()
    {
        return ConvertTo<TOrderedSourcePartitionStatePtr>(StateManager->Get("/source/v0"));
    }

    TMessageId GetMaxPersistedMessageId()
    {
        return RunInInvoker([&] {
            return Source->GetMaxPersistedMessageIdExclusive();
        });
    }

    void SetDraining(bool draining)
    {
        RunInInvoker([&] {
            auto dynamicSourceSpec = CloneYsonStruct(DynamicSourceSpec);
            dynamicSourceSpec->Draining = draining;
            auto dynamicSourceContext = New<TDynamicSourceContext>();
            dynamicSourceContext->DynamicSourceSpec = dynamicSourceSpec;
            dynamicSourceContext->DynamicPartitionSpec = DynamicSourcePartitionSpec;
            Source->Reconfigure(dynamicSourceContext);
        });
    }

    //! (Key, policy) entries of the read settings memory in the synced state.
    std::vector<std::pair<i64, ETestMalformedMessagePolicy>> GetReadSettingsMemory()
    {
        std::vector<std::pair<i64, ETestMalformedMessagePolicy>> entries;
        const auto state = GetState();
        if (const auto& memory = state->ReadSettingsMemory) {
            for (const auto& [offset, settings] : *memory) {
                entries.emplace_back(
                    OffsetToInt(offset),
                    ParseMalformedMessagePolicyReadSettings(ConvertToYsonString(settings.Node), ETestMalformedMessagePolicy::Keep));
            }
        }
        return entries;
    }

    static std::vector<TMessageId> GetIds(const std::vector<TUnpackedMessage>& data)
    {
        std::vector<TMessageId> ids;
        for (const auto& item : data) {
            ids.push_back(item.Message->MessageId);
        }
        return ids;
    }

    static std::vector<ui64> GetData(const std::vector<TUnpackedMessage>& data)
    {
        std::vector<ui64> values;
        for (const auto& item : data) {
            values.push_back(FromUnversionedValue<ui64>(item.Message->Payload.Underlying()[0]));
        }
        return values;
    }
};

TEST_F(TStableReadGroupsTest, FirstSightReadIsOneGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto readCalls = ReadConfig->ReadCallCount;
    const auto data = Read();
    EXPECT_EQ(data.size(), 5u);
    EXPECT_EQ(ReadConfig->ReadCallCount, readCalls + 1);
}

TEST_F(TStableReadGroupsTest, ReplayWithSmallerBatchReturnsWholeGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    Restart(/*maxOffset*/ 5);
    const auto readCalls = ReadConfig->ReadCallCount;
    EXPECT_EQ(GetIds(Read(/*maxRows*/ 2)), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallCount, readCalls + 3);
}

TEST_F(TStableReadGroupsTest, ReplayOfSplitReadReturnsWholeGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    ReadConfig->MaxRecordsPerCall = 2;
    Restart(/*maxOffset*/ 5);
    const auto readCalls = ReadConfig->ReadCallCount;
    EXPECT_EQ(GetIds(Read()), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallCount, readCalls + 3);
}

TEST_F(TStableReadGroupsTest, ReplayWithoutProgressKeepsBufferedRecords)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    // Only a part of the group is available after the restart.
    Restart(/*maxOffset*/ 3);
    const auto persistedMessageId = GetMaxPersistedMessageId();
    EXPECT_TRUE(Read().empty());
    Checkpoint({}, /*persist*/ false);
    EXPECT_EQ(GetMaxPersistedMessageId(), persistedMessageId);
    EXPECT_EQ(OffsetToInt(GetState()->PublishedOffsetExclusive), 5);
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 0);

    // The next call goes on from the buffered records.
    ReadConfig->ReadCallOffsets.clear();
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    EXPECT_EQ(GetIds(Read()), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{3}));

    // The buffer is not persisted: after a restart the group is read from its start.
    Restart(/*maxOffset*/ 3);
    EXPECT_TRUE(Read().empty());
    Restart(/*maxOffset*/ 5);
    ReadConfig->ReadCallOffsets.clear();
    EXPECT_EQ(GetIds(Read()), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{0}));
}

TEST_F(TStableReadGroupsTest, ReplayOverOffsetGapsReturnsSameRecords)
{
    ReadConfig->AbsentOffsets = {1, 3};
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    const auto data = Read();
    EXPECT_EQ(GetData(data), (std::vector<ui64>{0, 2, 4, 5}));
    Checkpoint(data, /*persist*/ false);

    // Every call returns one record, so the buffer has gaps between calls; the trim offset is below them.
    ReadConfig->MaxRecordsPerCall = 1;
    Restart(/*maxOffset*/ 6);
    ReadConfig->ReadCallOffsets.clear();
    EXPECT_EQ(GetIds(Read()), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{0, 1, 3, 5}));
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 2);
}

TEST_F(TStableReadGroupsTest, UnreportedTrimInsideReplayReturnsSuffix)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    // The second call of the replay sees the log trimmed up to 3 after offsets 0 and 1 are buffered.
    ReadConfig->MaxRecordsPerCall = 2;
    Restart(/*maxOffset*/ 5);
    ReadConfig->TrimAtCall = {ReadConfig->ReadCallCount + 2, 3};
    EXPECT_EQ(GetIds(Read()), (std::vector{data[3].Message->MessageId, data[4].Message->MessageId}));
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 1);
}

TEST_F(TStableReadGroupsTest, TrimPastGroupEndFoundByGapStopsAtGroupEnd)
{
    ReadConfig->AbsentOffsets = {2};
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    EXPECT_EQ(GetData(data), (std::vector<ui64>{0, 1, 3, 4}));
    Checkpoint(data, /*persist*/ false);

    // The connector still returns offset 3 after the gap, but the broker reports a trim past the group.
    ReadConfig->MaxRecordsPerCall = 1;
    ReadConfig->ReportedTrimmedOffset = 8;
    Restart(/*maxOffset*/ 10);
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 1);

    ReadConfig->ReadCallOffsets.clear();
    EXPECT_EQ(GetData(Read()), (std::vector<ui64>{5}));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{5}));
}

TEST_F(TStableReadGroupsTest, TrimOfUnreadGroupTailDiscardsGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    // The second call of the replay finds the log trimmed up to the group end and returns nothing.
    ReadConfig->MaxRecordsPerCall = 2;
    Restart(/*maxOffset*/ 10);
    ReadConfig->TrimAtCall = {ReadConfig->ReadCallCount + 2, 5};
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 1);

    ReadConfig->ReadCallOffsets.clear();
    EXPECT_EQ(GetData(Read()), (std::vector<ui64>{5, 6}));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{5}));
    Checkpoint({}, /*persist*/ false);
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 5);
}

TEST_F(TStableReadGroupsTest, ReplayWithoutProgressAndTrimAtCursorKeepsGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto first = Read();
    ASSERT_EQ(first.size(), 3u);
    Checkpoint(first, /*persist*/ true);
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    const auto second = Read();
    ASSERT_EQ(second.size(), 3u);
    Checkpoint(second, /*persist*/ false);

    ReadConfig->ReportedTrimmedOffset = 3;
    Restart(/*maxOffset*/ 4);
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 1);

    ReadConfig->ReadCallOffsets.clear();
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    EXPECT_EQ(GetIds(Read()), GetIds(second));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{4}));
}

TEST_F(TStableReadGroupsTest, FailedPullAndTrimFetchKeepReplayedGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    // A part of the group is buffered, then both the pull and the trim lookup fail.
    Restart(/*maxOffset*/ 3);
    ReadConfig->TrimmedOffsetFetchError = TError("Metadata request failed");
    EXPECT_TRUE(Read().empty());
    RunInInvoker([&] {
        Source->SetTestError(TError("Pull failed"));
    });
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(ReadConfig->TrimmedOffsetFetchCount, 2);

    // The same source instance goes on from the buffered records.
    ReadConfig->TrimmedOffsetFetchError = {};
    ReadConfig->ReadCallOffsets.clear();
    RunInInvoker([&] {
        Source->SetTestError({});
        Source->SetMaxOffset(5);
    });
    EXPECT_EQ(GetIds(Read()), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{3}));
}

TEST_F(TStableReadGroupsTest, ReplayTimeBoundKeepsBufferedRecords)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 5u);
    Checkpoint(data, /*persist*/ false);

    DefaultBatcherSettings->BatchDuration = TDuration::MilliSeconds(100);
    ReadConfig->MaxRecordsPerCall = 1;
    ReadConfig->ReadCallDelay = TDuration::MilliSeconds(60);
    Restart(/*maxOffset*/ 5);
    const auto readCalls = ReadConfig->ReadCallCount;
    EXPECT_TRUE(Read().empty());
    EXPECT_LE(ReadConfig->ReadCallCount, readCalls + 2);

    std::vector<TUnpackedMessage> replayed;
    for (int attempt = 0; attempt < 5 && replayed.empty(); ++attempt) {
        replayed = Read();
    }
    EXPECT_EQ(GetIds(replayed), GetIds(data));
    EXPECT_EQ(ReadConfig->ReadCallCount, readCalls + 5);
}

TEST_F(TStableReadGroupsTest, ReplayWithLargerBatchStopsAtGroupBoundary)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    const auto first = Read(/*maxRows*/ 3);
    const auto second = Read(/*maxRows*/ 3);
    ASSERT_EQ(first.size(), 3u);
    ASSERT_EQ(second.size(), 3u);
    Checkpoint(first, /*persist*/ false);
    Checkpoint(second, /*persist*/ false);

    Restart(/*maxOffset*/ 6);
    EXPECT_EQ(GetIds(Read()), GetIds(first));
    EXPECT_EQ(GetIds(Read()), GetIds(second));
    EXPECT_TRUE(Read().empty());
}

TEST_F(TStableReadGroupsTest, FirstBatchHoldsCursor)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 3u);
    RunInInvoker([&] {
        for (const auto& item : data) {
            Source->MarkPublished(item.Cookie);
        }
        Source->MarkPersisted(data[1].Cookie);
        Source->MarkPersisted(data[2].Cookie);
        Source->Sync();
        StateManager->Sync();
    });
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 0);

    RunInInvoker([&] {
        Source->MarkPersisted(data[0].Cookie);
        Source->Sync();
        StateManager->Sync();
    });
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 3);
    EXPECT_TRUE(GetState()->OffsetMemory->empty());
}

TEST_F(TStableReadGroupsTest, ReplayStartsAfterLeadingPayloadlessRecords)
{
    ReadConfig->PayloadlessOffsets = {0};
    RunInInvoker([&] {
        Source->SetMaxOffset(4);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 3u);
    Checkpoint(data, /*persist*/ false);
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 1);

    Restart(/*maxOffset*/ 4);
    EXPECT_EQ(GetIds(Read(/*maxRows*/ 2)), GetIds(data));
}

TEST_F(TStableReadGroupsTest, TrimIntoOpenGroup)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    Checkpoint(data, /*persist*/ false);

    Restart(/*maxOffset*/ 2);
    EXPECT_TRUE(Read().empty());
    // The buffered records 0 and 1 are trimmed together with offset 2.
    RunInInvoker([&] {
        Source->SetCommittedOffset(3);
        Source->SetMaxOffset(5);
    });
    EXPECT_EQ(GetIds(Read()), (std::vector{data[3].Message->MessageId, data[4].Message->MessageId}));
}

TEST_F(TStableReadGroupsTest, TrimPastOpenGroupDiscardsIt)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    Checkpoint(data, /*persist*/ false);

    Restart(/*maxOffset*/ 2);
    EXPECT_TRUE(Read().empty());
    RunInInvoker([&] {
        Source->SetMaxOffset(7);
        Source->SetCommittedOffset(7);
        // The trim is applied here, between two reads.
        Y_UNUSED(Source->BuildInflight());
    });
    ReadConfig->ReadCallOffsets.clear();
    EXPECT_TRUE(Read().empty());
    EXPECT_EQ(ReadConfig->ReadCallOffsets, (std::vector<i64>{7}));
    Checkpoint({}, /*persist*/ false);
    EXPECT_EQ(OffsetToInt(GetState()->PersistedOffsetExclusive), 7);
}

TEST_F(TStableReadGroupsTest, DrainWithTrimPastBufferedGroupSuspends)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    const auto data = Read();
    Checkpoint(data, /*persist*/ false);
    ASSERT_EQ(OffsetToInt(GetState()->PublishedOffsetExclusive), 5);

    Restart(/*maxOffset*/ 2);
    SetDraining(true);
    EXPECT_TRUE(Read().empty());
    const auto notSuspended = RunInInvoker([&] {
        return Source->BuildInflight();
    });
    EXPECT_FALSE(notSuspended->Suspended);

    RunInInvoker([&] {
        Source->SetCommittedOffset(5);
    });
    EXPECT_TRUE(Read().empty());
    const auto inflight = RunInInvoker([&] {
        return Source->BuildInflight();
    });
    EXPECT_TRUE(inflight->Suspended);
}

TEST_F(TStableReadGroupsTest, ReplayUsesRememberedSettingsAfterSwitchToDrop)
{
    ReadConfig->MalformedOffsets = {1};
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    ASSERT_EQ(data.size(), 3u);
    EXPECT_EQ(GetData(data), (std::vector<ui64>{0, 1 + MalformedDataShift, 2}));
    Checkpoint(data, /*persist*/ false);
    EXPECT_EQ(GetReadSettingsMemory(), (std::vector{std::pair(i64{2}, ETestMalformedMessagePolicy::Keep)}));

    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    Restart(/*maxOffset*/ 3);
    const auto replayed = Read();
    EXPECT_EQ(GetIds(replayed), GetIds(data));
    EXPECT_EQ(GetData(replayed), GetData(data));
    EXPECT_EQ(ReadConfig->AppliedPolicies.back(), ETestMalformedMessagePolicy::Keep);

    // Persisted groups are forgotten.
    Checkpoint(replayed, /*persist*/ true);
    EXPECT_TRUE(GetReadSettingsMemory().empty());
}

TEST_F(TStableReadGroupsTest, ReplayUsesRememberedSettingsAfterSwitchToKeep)
{
    ReadConfig->MalformedOffsets = {1};
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    EXPECT_EQ(GetData(data), (std::vector<ui64>{0, 2}));
    Checkpoint(data, /*persist*/ false);

    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Keep;
    Restart(/*maxOffset*/ 3);
    const auto replayed = Read();
    EXPECT_EQ(GetIds(replayed), GetIds(data));
    EXPECT_EQ(GetData(replayed), GetData(data));
}

TEST_F(TStableReadGroupsTest, PolicyChangeInsideReadCallUsesCallSnapshot)
{
    ReadConfig->MalformedOffsets = {0, 2};
    ReadConfig->PolicyAfterFirstMalformed = ETestMalformedMessagePolicy::Drop;
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    EXPECT_EQ(GetData(data), (std::vector<ui64>{MalformedDataShift, 1, 2 + MalformedDataShift}));
    Checkpoint(data, /*persist*/ false);

    ReadConfig->PolicyAfterFirstMalformed.reset();
    Restart(/*maxOffset*/ 3);
    const auto replayed = Read();
    EXPECT_EQ(GetIds(replayed), GetIds(data));
    EXPECT_EQ(GetData(replayed), GetData(data));
}

TEST_F(TStableReadGroupsTest, ReadsWithEqualSettingsShareEntry)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    std::vector<std::vector<TUnpackedMessage>> groups;
    groups.push_back(Read(/*maxRows*/ 2));
    groups.push_back(Read(/*maxRows*/ 2));
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    groups.push_back(Read(/*maxRows*/ 2));
    for (const auto& group : groups) {
        Checkpoint(group, /*persist*/ false);
    }

    EXPECT_EQ(
        GetReadSettingsMemory(),
        (std::vector{std::pair(i64{3}, ETestMalformedMessagePolicy::Keep), std::pair(i64{5}, ETestMalformedMessagePolicy::Drop)}));
}

TEST_F(TStableReadGroupsTest, ReadSettingsOfPersistedGroupsAreForgotten)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    std::vector<std::vector<TUnpackedMessage>> groups;
    groups.push_back(Read(/*maxRows*/ 2));
    groups.push_back(Read(/*maxRows*/ 2));
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    groups.push_back(Read(/*maxRows*/ 2));

    // The entry at 3 covers the second group until it is persisted.
    Checkpoint(groups[0], /*persist*/ true);
    EXPECT_EQ(std::ssize(GetReadSettingsMemory()), 2);
    Checkpoint(groups[1], /*persist*/ true);
    EXPECT_EQ(GetReadSettingsMemory(), (std::vector{std::pair(i64{5}, ETestMalformedMessagePolicy::Drop)}));
    Checkpoint(groups[2], /*persist*/ true);
    EXPECT_TRUE(GetReadSettingsMemory().empty());
}

class TStableReadGroupsCursorJumpTest
    : public TStableReadGroupsTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TStableReadGroupsCursorJumpTest, LookupByEntryKeyAfterCursorJump)
{
    const bool reposition = GetParam();
    ReadConfig->MalformedOffsets = {4};
    RunInInvoker([&] {
        Source->SetMaxOffset(5);
    });
    Checkpoint(Read(/*maxRows*/ 2), /*persist*/ false);
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    const auto second = Read(/*maxRows*/ 3);
    EXPECT_EQ(GetData(second), (std::vector<ui64>{2, 3}));
    Checkpoint(second, /*persist*/ false);

    // The cursor jumps into the second group, whose settings are registered under its entry key 4.
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Keep;
    Restart(/*maxOffset*/ 5);
    RunInInvoker([&] {
        if (reposition) {
            Source->Reposition(3);
        } else {
            Source->SetCommittedOffset(3);
        }
    });
    const auto replayed = Read();
    EXPECT_EQ(GetData(replayed), (std::vector<ui64>{3}));
    EXPECT_EQ(GetIds(replayed), (std::vector{second[1].Message->MessageId}));
}

INSTANTIATE_TEST_SUITE_P(
    TrimAndReposition,
    TStableReadGroupsCursorJumpTest,
    ::testing::Values(false, true));

TEST_F(TStableReadGroupsTest, ReplayWithoutRememberedSettingsUsesCurrentOnes)
{
    // The existing reader registers groups but no settings.
    ReadConfig->MalformedOffsets = {1};
    Restart(/*maxOffset*/ 3, /*stable*/ false);
    const auto data = Read();
    EXPECT_EQ(GetData(data), (std::vector<ui64>{0, 1 + MalformedDataShift, 2}));
    Checkpoint(data, /*persist*/ false);
    EXPECT_TRUE(GetReadSettingsMemory().empty());

    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    Restart(/*maxOffset*/ 3);
    const auto replayed = Read();
    EXPECT_EQ(GetData(replayed), (std::vector<ui64>{0, 2}));
    Checkpoint(replayed, /*persist*/ false);
    EXPECT_TRUE(GetReadSettingsMemory().empty());
}

TEST_F(TStableReadGroupsTest, NonStableReaderAppliesCapturedSettingsPerCall)
{
    Restart(/*maxOffset*/ 4, /*stable*/ false);
    ReadConfig->AppliedPolicies.clear();
    Read(/*maxRows*/ 1);
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Drop;
    Read(/*maxRows*/ 1);
    EXPECT_EQ(
        ReadConfig->AppliedPolicies,
        (std::vector{ETestMalformedMessagePolicy::Keep, ETestMalformedMessagePolicy::Drop}));
}

TEST_F(TStableReadGroupsTest, FailedReadRemembersNothing)
{
    ReadConfig->MalformedOffsets = {1};
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Fail;
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    EXPECT_THROW_WITH_SUBSTRING(Read(), "Critical read failure");
    RunInInvoker([&] {
        Source->Sync();
        StateManager->Sync();
    });
    EXPECT_TRUE(GetState()->OffsetMemory->empty());
    EXPECT_TRUE(GetReadSettingsMemory().empty());

    // After the operator switches to `keep`, the retry is a first-sight read under `keep`.
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Keep;
    EXPECT_EQ(GetData(Read()), (std::vector<ui64>{0, 1 + MalformedDataShift, 2}));
}

TEST_F(TStableReadGroupsTest, FailPrefixIsRememberedAndReplayed)
{
    ReadConfig->MalformedOffsets = {1};
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Fail;
    ReadConfig->FailCutsRead = true;
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto prefix = Read();
    EXPECT_EQ(GetData(prefix), (std::vector<ui64>{0}));

    // After the switch to `keep`, the next read is first-sight and remembers `keep`.
    ReadConfig->MalformedMessagePolicy = ETestMalformedMessagePolicy::Keep;
    const auto rest = Read();
    EXPECT_EQ(GetData(rest), (std::vector<ui64>{1 + MalformedDataShift, 2}));
    Checkpoint(prefix, /*persist*/ false);
    Checkpoint(rest, /*persist*/ false);
    EXPECT_EQ(
        GetReadSettingsMemory(),
        (std::vector{std::pair(i64{0}, ETestMalformedMessagePolicy::Fail), std::pair(i64{2}, ETestMalformedMessagePolicy::Keep)}));

    // The prefix group is replayed under `fail` and holds no malformed record, so it is identical.
    Restart(/*maxOffset*/ 3);
    EXPECT_EQ(GetIds(Read()), GetIds(prefix));
    const auto replayedRest = Read();
    EXPECT_EQ(GetIds(replayedRest), GetIds(rest));
    EXPECT_EQ(GetData(replayedRest), GetData(rest));
}

TEST_F(TStableReadGroupsTest, OvershootPastGroupEndFailsTheRead)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(6);
    });
    Checkpoint(Read(/*maxRows*/ 3), /*persist*/ false);
    Checkpoint(Read(/*maxRows*/ 3), /*persist*/ false);

    ReadConfig->IgnoreOffsetLimit = true;
    Restart(/*maxOffset*/ 6);
    EXPECT_THROW_WITH_SUBSTRING(Read(), "past the end");
}

TEST_F(TStableReadGroupsTest, RecordsBeforeReadStartAreDropped)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    Checkpoint(data, /*persist*/ false);

    ReadConfig->ReadStartRewind = 2;
    Restart(/*maxOffset*/ 3);
    // With one row per call the connector returns only the record at offset 0 after the first call.
    EXPECT_TRUE(Read(/*maxRows*/ 1).empty());
    EXPECT_EQ(GetIds(Read()), GetIds(data));
}

TEST_F(TStableReadGroupsTest, ReadWithOnlyRecordsBeforeReadStartIsEmpty)
{
    RunInInvoker([&] {
        Source->SetMaxOffset(3);
    });
    const auto data = Read();
    Checkpoint(data, /*persist*/ false);

    ReadConfig->ReadStartRewind = 1;
    Restart(/*maxOffset*/ 3);
    const auto readCalls = ReadConfig->ReadCallCount;
    // The second call returns only the record at offset 0, which was read already.
    EXPECT_TRUE(Read(/*maxRows*/ 1).empty());
    EXPECT_EQ(ReadConfig->ReadCallCount, readCalls + 2);
    EXPECT_EQ(GetIds(Read()), GetIds(data));
}

TEST_F(TStableReadGroupsTest, StableReadGroupsRequireDeclaration)
{
    EXPECT_TRUE(TRegistry::Get()->IsSourceReplayStable(TypeName<TTestSource>()));
    EXPECT_FALSE(TRegistry::Get()->IsSourceReplayStable(TypeName<TUndeclaredTestSource>()));
    EXPECT_FALSE(TRegistry::Get()->IsSourceReplayStable("NYT::NFlow::TUnknownSource"));

    auto spec = CloneYsonStruct(SourceSpec);
    spec->SourceClassName = TypeName<TUndeclaredTestSource>();
    SourceContext->SourceSpec = spec;
    auto undeclared = New<TUndeclaredTestSource>(SourceContext, MakeDynamicSourceContext());
    EXPECT_THROW_WITH_SUBSTRING(undeclared->EnableStableReadGroups(), "does not support stable read groups");
    SourceContext->SourceSpec = SourceSpec;
}

TEST(TMalformedMessagePolicyReadSettingsTest, ParseRules)
{
    using EPolicy = ETestMalformedMessagePolicy;
    EXPECT_EQ(ParseMalformedMessagePolicyReadSettings(TYsonString(), EPolicy::Drop), EPolicy::Drop);
    EXPECT_EQ(
        ParseMalformedMessagePolicyReadSettings(MakeMalformedMessagePolicyReadSettings(EPolicy::Fail), EPolicy::Keep),
        EPolicy::Fail);
    // A missing key is the registered default, and an unknown key is ignored.
    EXPECT_EQ(ParseMalformedMessagePolicyReadSettings(TYsonString(TStringBuf("{}")), EPolicy::Drop), EPolicy::Drop);
    EXPECT_EQ(
        ParseMalformedMessagePolicyReadSettings(TYsonString(TStringBuf("{malformed_message_policy=keep; future_setting=1}")), EPolicy::Drop),
        EPolicy::Keep);
    EXPECT_THROW_WITH_SUBSTRING(
        ParseMalformedMessagePolicyReadSettings(TYsonString(TStringBuf("{malformed_message_policy=quarantine}")), EPolicy::Drop),
        "Unknown value of read setting");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseMalformedMessagePolicyReadSettings(TYsonString(TStringBuf("keep")), EPolicy::Drop),
        "Unknown value of read setting");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
