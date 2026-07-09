#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/seq_no_provider.h>
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
        TDynamicSourceContextPtr dynamicContext)
        : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
        , Schema_(
            New<TTableSchema>(std::vector{
                TColumnSchema("data", EValueType::Uint64),
            }))
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{.MaxOffsetExclusive = IntToOffset(MaxOffsetExclusive_)});
    }

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

    void SetTestError(TError error)
    {
        Error_ = std::move(error);
    }

    void SetTestWriteTimestamps(std::vector<ui64> timestamps)
    {
        WriteTimestamps_ = std::move(timestamps);
    }

private:
    TFuture<std::vector<TRecord>> DoReadNextBatch(
        const TMessageBatcherSettingsPtr& settings,
        TOffset nextOffsetAsKey,
        std::optional<TOffset> offsetLimitOptionalAsKey) final
    {
        i64 nextOffset = OffsetToInt(nextOffsetAsKey);
        std::optional<i64> offsetLimitOptional = offsetLimitOptionalAsKey ? std::optional(OffsetToInt(*offsetLimitOptionalAsKey)) : std::nullopt;

        GetReadErrorState()->SetError(Error_);

        std::vector<TRecord> records;
        if (!Error_.IsOK()) {
            return MakeFuture(records);
        }

        const i64 offsetLimit = std::min({
            offsetLimitOptional.value_or(std::numeric_limits<i64>::max()),
            nextOffset + settings->MaxRowsPerBatch,
            MaxOffsetExclusive_,
        });

        TPayloadBuilder builder(Schema_);
        while (nextOffset < offsetLimit) {
            builder.SetValue(MakeUnversionedUint64Value(nextOffset), "data");

            auto writeTimestamp = nextOffset < std::ssize(WriteTimestamps_) ? TSystemTimestamp(WriteTimestamps_[nextOffset]) : TSystemTimestamp(nextOffset + 1);

            TRecord record = {
                .Offset = IntToOffset(nextOffset),
                .WriteTimestamp = writeTimestamp,
                .CreateTimestamp = TSystemTimestamp(nextOffset + 1),
                .Payloads = {builder.Finish()},
                .PayloadSchema = builder.GetSchema(),
            };

            records.push_back(record);
            nextOffset += 1;
        }
        return BIND(
            [records] () {
                for (int i = 0; i < 10; ++i) { // Imitate delay without wasting time.
                    NConcurrency::Yield();
                }
                return records;
            })
            .AsyncVia(GetContext()->SerializedInvoker)
            .Run();
    }

    void DoReportPersistedOffset(TOffset offsetExclusive) final
    {
        UpdatePartitionInfo(TPartitionInfoUpdate{.CommittedOffsetExclusive = offsetExclusive});
    }

private:
    const TTableSchemaPtr Schema_;
    i64 MaxOffsetExclusive_ = 0;
    TError Error_;
    std::vector<ui64> WriteTimestamps_;
};

DEFINE_REFCOUNTED_TYPE(TTestSource);

YT_FLOW_DEFINE_SOURCE(TTestSource);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTestSeqNoProvider);

struct TTestSeqNoProvider
    : public IUniqueSeqNoProvider
{
    mutable std::atomic<TUniqueSeqNo::TUnderlying> CurrentSeqNo = 1ULL << 63;

    TTestSeqNoProvider() = default;

    TFuture<TResult> Generate() const override
    {
        return MakeFuture(TResult{
            .Timestamp = TSystemTimestamp(TInstant::Now().Seconds()),
            .UniqueSeqNo = TUniqueSeqNo{CurrentSeqNo.fetch_add(1)},
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTestSeqNoProvider);

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

    TTestSourcePtr MakeTestSource(const TSourceSpecPtr& spec)
    {
        SourceContext->SourceSpec = spec;
        return New<TTestSource>(SourceContext, MakeDynamicSourceContext());
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

        SourceContext = New<TSourceContext>();

        SourceContext->SerializedInvoker = ActionQueue->GetInvoker();

        SourceContext->Partition = ConvertTo<TPartitionPtr>(NYson::TYsonString(TStringBuf(R"""({
            "partition_id" = "48946f5e-ac1b2be7-4babe692-8af11700";
            "computation_id" = "48946f5e-ac1b2be7-4babe692-8af11700";
            "parameters" = {};
            "state_epoch" = 0;
            "state_timestamp" = "2020-01-01T00:00:00Z";
        })""")));

        SourceContext->Logger = NLogging::TLogger("Test");
        SourceContext->StatusProfiler = CreateStatusProfiler();

        SourceContext->UniqueSeqNoProvider = New<TTestSeqNoProvider>();

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
                "unavailable_time_half_decay_period" = 1000;
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
