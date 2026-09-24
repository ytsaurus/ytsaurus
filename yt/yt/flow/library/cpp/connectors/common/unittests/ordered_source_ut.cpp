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

DECLARE_REFCOUNTED_CLASS(TTestSource);

class TTestSource
    : public TIntegerOffsetOrderedSourceBase
{
public:
    using TSourceController = TTestSourceController;

    using TOrderedSourceBase::TPartitionInfoUpdate;

    TTestSource(
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
    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffsetAsKey,
        std::optional<TOffset> offsetLimitOptionalAsKey) final
    {
        auto nextPosition = OffsetToInt(nextOffsetAsKey);
        std::optional<i64> offsetLimitPosition;
        if (offsetLimitOptionalAsKey) {
            offsetLimitPosition = OffsetToInt(*offsetLimitOptionalAsKey);
        }

        ReadErrorState_->SetError(Error_);

        std::vector<TRecord> records;
        if (!Error_.IsOK()) {
            return MakeFuture(records);
        }

        TPayloadBuilder builder(Schema_);
        while (std::ssize(records) < settings->MaxRowsPerBatch &&
            nextPosition < MaxOffsetExclusive_ &&
            (!offsetLimitPosition || nextPosition < *offsetLimitPosition))
        {
            builder.SetValue(MakeUnversionedUint64Value(nextPosition), "data");

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
            [records = std::move(records)] () mutable {
                for (int i = 0; i < 10; ++i) { // Imitate delay without wasting time.
                    NConcurrency::Yield();
                }
                return std::move(records);
            })
            .AsyncVia(GetContext()->SerializedInvoker)
            .Run();
    }

    void DoReportPersistedOffset(TOffset offsetExclusive) final
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

DEFINE_REFCOUNTED_TYPE(TTestSource);

YT_FLOW_DEFINE_SOURCE(TTestSource);

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

} // namespace
} // namespace NYT::NFlow
