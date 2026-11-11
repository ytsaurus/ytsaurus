#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/stores/output_store.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/serializer/state.h>

#include <yt/yt/flow/library/cpp/tables/partition_states.h>
#include <yt/yt/flow/library/cpp/tables/state.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <atomic>
#include <memory>
#include <utility>

namespace NYT::NFlow {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

namespace {

const TStreamId OutputStreamId("output");
const TUniqueSeqNo JobGeneration(42);

using TWarmupStateKey = NTables::IPartitionStates::TTableKey;

struct TBufferWarmupTransactionProbe
{
    struct TChange
    {
        TWarmupStateKey Key;
        std::optional<TUnversionedOwningRow> Row;
    };

    struct TAttempt
    {
        TTimestamp StartTimestamp = TTimestamp(0);
        bool TimestampWritten = false;
        bool HasDelete = false;
        std::vector<TChange> Changes;
    };

    const TPartitionId PartitionId = TPartitionId(TGuid::Create());
    const TPartitionId OtherPartitionId = TPartitionId(TGuid::Create());
    const TLeaseId LeaseId = TLeaseId(TGuid::Create());
    const TTableSchemaPtr StateSchema = MakeStateSchema();
    THashMap<TWarmupStateKey, TUnversionedOwningRow> Rows;
    std::vector<TWarmupStateKey> StateLookups;
    std::vector<TWarmupStateKey> AttemptedDeletes;
    std::vector<TWarmupStateKey> CommittedDeletes;
    std::optional<TTimestamp> LastCommittedTimestamp;
    TError NextCleanupError;
    bool CommitBeforeError = false;
    int FailedCleanupCommits = 0;
    int TimestampLookups = 0;
    ui64 NextTimestamp = 1;

    TWarmupStateKey LegacyKey() const
    {
        return {PartitionId, LegacyBufferWarmupStateName};
    }

    TWarmupStateKey CanonicalKey() const
    {
        return {PartitionId, BufferWarmupStateName};
    }

    static TTableSchemaPtr MakeStateSchema()
    {
        std::vector<TColumnSchema> columns{
            TColumnSchema("partition_id", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("name", EValueType::String, ESortOrder::Ascending),
        };
        const auto schema = NYsonSerializer::GetYsonStateSchema<NTables::TInternalState>();
        for (const auto& column : schema->TableSchema->Columns()) {
            columns.push_back(column);
        }
        return New<TTableSchema>(std::move(columns));
    }

    static TUnversionedValue GetCell(
        const TNameTablePtr& nameTable,
        TUnversionedRow row,
        TStringBuf name)
    {
        const auto id = nameTable->GetIdOrThrow(name);
        for (const auto& value : row) {
            if (value.Id == id) {
                return value;
            }
        }
        YT_ABORT();
    }

    static TWarmupStateKey DecodeKey(
        const TNameTablePtr& nameTable,
        TUnversionedRow row)
    {
        return {
            FromUnversionedValue<TPartitionId>(GetCell(nameTable, row, "partition_id")),
            FromUnversionedValue<std::string>(GetCell(nameTable, row, "name")),
        };
    }

    TUnversionedOwningRow CaptureStateUpdate(
        const TNameTablePtr& nameTable,
        TUnversionedRow row) const
    {
        TUnversionedOwningRowBuilder builder;
        for (auto value : row) {
            value.Id = StateSchema->GetColumnIndexOrThrow(nameTable->GetName(value.Id));
            builder.AddValue(value);
        }
        return builder.FinishRow();
    }

    void ApplyUpdate(const TWarmupStateKey& key, TUnversionedRow update)
    {
        std::vector<TUnversionedValue> values;
        for (int index = 0; index < std::ssize(StateSchema->Columns()); ++index) {
            values.push_back(MakeUnversionedNullValue(index));
        }
        if (auto it = Rows.find(key); it != Rows.end()) {
            for (const auto& value : it->second) {
                values[value.Id] = value;
            }
        }
        for (const auto& value : update) {
            values[value.Id] = value;
        }
        TUnversionedOwningRowBuilder builder;
        for (const auto& value : values) {
            builder.AddValue(value);
        }
        Rows[key] = builder.FinishRow();
    }

    void Seed(const TWarmupStateKey& key, TYsonString payload)
    {
        const auto schema = NYsonSerializer::GetYsonStateSchema<NTables::TInternalState>();
        auto state = New<NYsonSerializer::TState>(schema);
        state->Init();
        auto value = New<NTables::TInternalState>();
        value->State = std::move(payload);
        state->SetValue(value);
        auto mutation = state->FlushMutation();
        const auto& update = std::get<NYsonSerializer::TUpdateMutation>(mutation);
        auto nameTable = TNameTable::FromSchema(*schema->TableSchema);
        auto captured = CaptureStateUpdate(nameTable, update);
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedStringValue(ToString(key.PartitionId), 0));
        builder.AddValue(MakeUnversionedStringValue(key.Name, 1));
        for (const auto& cell : captured) {
            builder.AddValue(cell);
        }
        auto row = builder.FinishRow();
        ApplyUpdate(key, row);
    }

    TYsonString ReadState(const TWarmupStateKey& key) const
    {
        const auto schema = NYsonSerializer::GetYsonStateSchema<NTables::TInternalState>();
        auto state = New<NYsonSerializer::TState>(schema);
        state->Init(Rows.at(key), NYsonSerializer::PrepareMapping(schema, StateSchema));
        auto value = state->GetValueAs<NTables::TInternalState>();
        YT_VERIFY(value->State);
        return *value->State;
    }

    TUnversionedLookupRowsResult Lookup(
        const NYPath::TYPath& path,
        const TNameTablePtr& nameTable,
        const TSharedRange<TLegacyKey>& keys)
    {
        auto owningRows = std::make_shared<std::vector<TUnversionedOwningRow>>();
        std::vector<TUnversionedRow> rows;
        TTableSchemaPtr schema;
        if (path == "//pipeline/partition_states") {
            schema = StateSchema;
            for (const auto& row : keys) {
                auto key = DecodeKey(nameTable, row);
                StateLookups.push_back(key);
                if (auto it = Rows.find(key); it != Rows.end()) {
                    owningRows->push_back(it->second);
                    rows.push_back(owningRows->back());
                } else {
                    rows.emplace_back();
                }
            }
        } else {
            YT_VERIFY(path == "//pipeline/partition_transactions");
            ++TimestampLookups;
            schema = New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("partition_id", EValueType::String, ESortOrder::Ascending),
                TColumnSchema("last_transaction_start_timestamp", EValueType::Uint64),
            });
            for (const auto& key : keys) {
                EXPECT_EQ(
                    FromUnversionedValue<TPartitionId>(GetCell(nameTable, key, "partition_id")),
                    PartitionId);
                if (LastCommittedTimestamp) {
                    TUnversionedOwningRowBuilder builder;
                    builder.AddValue(MakeUnversionedStringValue(ToString(PartitionId), 0));
                    builder.AddValue(MakeUnversionedUint64Value(LastCommittedTimestamp->Underlying(), 1));
                    owningRows->push_back(builder.FinishRow());
                    rows.push_back(owningRows->back());
                } else {
                    rows.emplace_back();
                }
            }
        }
        return {
            .Rowset = CreateRowset(std::move(schema), MakeSharedRange(std::move(rows), owningRows)),
        };
    }

    void ApplyCommitted(const TAttempt& attempt)
    {
        for (const auto& change : attempt.Changes) {
            if (change.Row) {
                ApplyUpdate(change.Key, *change.Row);
            } else {
                Rows.erase(change.Key);
                CommittedDeletes.push_back(change.Key);
            }
        }
        LastCommittedTimestamp = attempt.StartTimestamp;
    }

    void Install(TMockClient* client)
    {
        ON_CALL(*client, LookupRows(_, _, _, _))
            .WillByDefault([this] (
                const NYPath::TYPath& path,
                TNameTablePtr nameTable,
                const TSharedRange<TLegacyKey>& keys,
                const TLookupRowsOptions&) {
                return MakeFuture(Lookup(path, nameTable, keys));
            });
        ON_CALL(*client, StartTransaction(_, _))
            .WillByDefault([this] (auto, const auto&) -> TFuture<ITransactionPtr> {
                auto attempt = std::make_shared<TAttempt>();
                attempt->StartTimestamp = TTimestamp(NextTimestamp++);
                auto transaction = New<NiceMock<TMockTransaction>>();
                transaction->StartTimestamp = attempt->StartTimestamp;
                ON_CALL(*transaction, ModifyRows(_, _, _, _))
                    .WillByDefault([this, attempt] (
                        const NYPath::TYPath& path,
                        TNameTablePtr nameTable,
                        const TSharedRange<TRowModification>& modifications,
                        const TModifyRowsOptions&) {
                        for (const auto& modification : modifications) {
                            if (path == "//pipeline/partition_states") {
                                if (const auto* erase = std::get_if<NRowModifications::TDeleteRow>(&modification)) {
                                    auto key = DecodeKey(nameTable, erase->Key);
                                    AttemptedDeletes.push_back(key);
                                    attempt->Changes.push_back({key, std::nullopt});
                                    attempt->HasDelete = true;
                                } else {
                                    const auto& write = std::get<NRowModifications::TWriteRow>(modification);
                                    attempt->Changes.push_back({
                                        DecodeKey(nameTable, write.Row),
                                        CaptureStateUpdate(nameTable, write.Row),
                                    });
                                }
                            } else {
                                YT_VERIFY(path == "//pipeline/partition_transactions");
                                const auto& write = std::get<NRowModifications::TWriteRow>(modification);
                                EXPECT_EQ(
                                    GetCell(nameTable, write.Row, "last_transaction_start_timestamp").Data.Uint64,
                                    attempt->StartTimestamp.Underlying());
                                attempt->TimestampWritten = true;
                            }
                        }
                    });
                ON_CALL(*transaction, Commit(_))
                    .WillByDefault([this, attempt] (const TTransactionCommitOptions& options) {
                        EXPECT_THAT(options.PrerequisiteTransactionIds, ElementsAre(LeaseId));
                        EXPECT_TRUE(attempt->TimestampWritten);
                        TError error;
                        if (attempt->HasDelete) {
                            error = std::exchange(NextCleanupError, TError());
                            if (!error.IsOK()) {
                                ++FailedCleanupCommits;
                            }
                        }
                        if (error.IsOK() || CommitBeforeError) {
                            ApplyCommitted(*attempt);
                        }
                        if (!error.IsOK()) {
                            return MakeFuture<TTransactionCommitResult>(error);
                        }
                        return MakeFuture(TTransactionCommitResult{});
                    });
                return MakeFuture<ITransactionPtr>(transaction);
            });
    }
};

class TRecordingBufferWarmupState
    : public IPartitionBufferState
{
public:
    bool Enabled = true;
    TDuration RefreshPeriod = TDuration::Zero();
    TPartitionBufferWarmup CurrentWarmup;
    std::vector<TPartitionBufferWarmup> Seeds;
    TStreamLimitUsageStateMap OutputStates{
        {OutputStreamId, New<TStreamLimitUsageState>()},
    };

    void SeedWarmup(const TPartitionBufferWarmup& warmup) override
    {
        Seeds.push_back(warmup);
    }

    TPartitionBufferWarmup GetWarmup() override
    {
        return CurrentWarmup;
    }

    bool IsWarmupEnabled() override
    {
        return Enabled;
    }

    TDuration GetWarmupRefreshPeriod() override
    {
        return RefreshPeriod;
    }

    const TStreamLimitUsageStateMap& GetOutputStreamLimitUsageStates() const override
    {
        return OutputStates;
    }
};

struct TSinkInitProbe
{
    enum class EEventType
    {
        Construct,
        Init,
        Prepare,
        Distribute,
        Watermark,
        PreSyncFlush,
        Sync,
        ContextCommit,
        Commit,
        Terminate,
    };

    struct TEvent
    {
        EEventType Type;
        TSinkId SinkId;
        const ISink* Instance = nullptr;
        TDynamicSinkSpecPtr DynamicSinkSpec;
        TUniqueSeqNo Generation;
    };

    std::vector<TEvent> Events;
    bool FailInit = false;
    bool FailPreSyncFlush = false;
    bool MismatchedParentKey = false;
    bool DistributeOnPrepare = false;
    bool RunOneEpoch = false;
    bool CaptureProcessingStatus = false;
    std::vector<TComputationStatusPtr> ProcessingStatuses;
    bool PersistOutputMessages = false;
    bool AckOnDistribute = true;
    bool AckInPreSyncFlush = false;
    int OutputWriteCalls = 0;
    bool CancelDuringPrepare = false;
    TPromise<void> PrepareStarted = NewPromise<void>();
    TPromise<void> PrepareBlocked = NewPromise<void>();
    ssize_t TerminationsBeforeDestruction = 0;
    std::shared_ptr<TBufferWarmupTransactionProbe> WarmupTransactions;
    IPartitionBufferStatePtr PartitionBufferState;
    bool ClearBufferStateBeforeRun = false;
    int EpochCount = 1;
};

std::atomic<TSinkInitProbe*>& GetActiveSinkInitProbe()
{
    static std::atomic<TSinkInitProbe*> Probe = nullptr;
    return Probe;
}

class TRecordingSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

class TRecordingSink
    : public ISink
{
public:
    using TSinkController = TRecordingSinkController;

    TRecordingSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext)
        : Context_(std::move(context))
        , DynamicSinkSpec_(dynamicContext->DynamicSinkSpec)
        , Probe_(GetActiveSinkInitProbe().load())
    {
        YT_VERIFY(Probe_);
        Record(TSinkInitProbe::EEventType::Construct);
    }

    void Init(IInitContextPtr /*initContext*/) override
    {
        Record(TSinkInitProbe::EEventType::Init);
        THROW_ERROR_EXCEPTION_IF(Probe_->FailInit, "Recording sink init failed");
    }

    void Distribute(
        const TOutputMessageConstPtr& /*message*/,
        TOnDistributedCallback onDistributed) override
    {
        Record(TSinkInitProbe::EEventType::Distribute);
        if (Probe_->AckOnDistribute) {
            onDistributed();
        } else {
            Callbacks_.push_back(std::move(onDistributed));
        }
    }

    void PreSyncFlush() override
    {
        Record(TSinkInitProbe::EEventType::PreSyncFlush);
        auto* traceContext = NTracing::TryGetCurrentTraceContext();
        ASSERT_TRUE(traceContext);
        EXPECT_THAT(traceContext->GetSpanName(), EndsWith(".PreSyncFlush"));
        THROW_ERROR_EXCEPTION_IF(Probe_->FailPreSyncFlush, "Recording sink pre-sync flush failed");
        if (Probe_->AckInPreSyncFlush) {
            for (auto& callback : Callbacks_) {
                callback();
            }
            Callbacks_.clear();
        }
    }

    void Sync(IDynamicTableTransactionPtr /*transaction*/) override
    {
        Record(TSinkInitProbe::EEventType::Sync);
    }

    void Commit() override
    {
        Record(TSinkInitProbe::EEventType::Commit);
    }

    void Terminate() override
    {
        Record(TSinkInitProbe::EEventType::Terminate);
    }

    void UpdateWatermarkState(TWatermarkStatePtr /*state*/) override
    {
        Record(TSinkInitProbe::EEventType::Watermark);
    }

private:
    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }

    void Record(TSinkInitProbe::EEventType type)
    {
        Probe_->Events.push_back({
            .Type = type,
            .SinkId = Context_->SinkId,
            .Instance = this,
            .DynamicSinkSpec = DynamicSinkSpec_,
            .Generation = Context_->JobGeneration,
        });
    }

private:
    std::vector<TOnDistributedCallback> Callbacks_;
    const TSinkContextPtr Context_;
    const TDynamicSinkSpecPtr DynamicSinkSpec_;
    TSinkInitProbe* const Probe_;
};

YT_FLOW_DEFINE_SINK(TRecordingSink);

class TTestComputationRunContext
    : public IComputationRunContext
{
public:
    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(
        const THashSet<TStreamId>& /*allowedStreams*/) override
    {
        return MakeFuture(std::vector<TInputMessageConstPtr>{});
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        return MakeFuture(THashMap<TStreamId, TInflightMetricsPtr>{});
    }

    void RegisterSourceMessages(i64 /*count*/) override
    { }

    void MarkPersisted(std::span<const TMessageId> /*messageIds*/) override
    { }

    void MarkDeduplicated(std::span<const TMessageId> /*messageIds*/) override
    { }

    void RegisterOutputMessages(
        std::span<const TOutputMessageConstPtr> /*messages*/,
        std::span<TDistributingTracker> /*trackers*/) override
    { }

    void Commit() override
    {
        GetActiveSinkInitProbe().load()->Events.push_back({
            .Type = TSinkInitProbe::EEventType::ContextCommit,
        });
    }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }
};

class TProductionWiredSinkInitComputation
    : public TUniversalComputationBase
{
public:
    TProductionWiredSinkInitComputation(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext)
        : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
        , Probe_(GetActiveSinkInitProbe().load())
    {
        YT_VERIFY(Probe_);
    }

    void RunForTest(const IComputationRunContextPtr& context)
    {
        Run(context);
    }

private:
    void DoPrepare(const IComputationRunContextPtr& context) override
    {
        Probe_->Events.push_back({
            .Type = TSinkInitProbe::EEventType::Prepare,
        });

        if (Probe_->CancelDuringPrepare) {
            Probe_->PrepareStarted.Set();
            WaitFor(Probe_->PrepareBlocked.ToFuture())
                .ThrowOnError();
        }

        if (Probe_->DistributeOnPrepare) {
            const auto& schema = GetContext()->StreamSpecStorage->GetSchema(OutputStreamId);
            TMessageBuilder builder(OutputStreamId, schema);
            builder.SetMessageId(TMessageId("message"));
            builder.SetSystemTimestamp(TSystemTimestamp(1));
            builder.SetEventTimestamp(TSystemTimestamp(1));
            builder.SetAlignmentTimestamp(TSystemTimestamp(1));
            std::vector<TOutputMessageConstPtr> messages{
                New<TOutputMessage>(builder.Finish(), GetContext()->StreamSpecStorage),
            };
            if (Probe_->PersistOutputMessages) {
                OutputStore_->RegisterBatch(messages);
            }
            RegisterOutputMessages(
                context,
                messages,
                Probe_->MismatchedParentKey
                    ? std::optional<TKey>(MakeKey("other-key"))
                    : GetContext()->Partition->SourceKey);
        }

        if (!Probe_->RunOneEpoch) {
            THROW_ERROR_EXCEPTION("Sink init test stopped after DoPrepare");
        }
    }

    void DoExecute(
        const IComputationRunContextPtr& context,
        NTracing::TTraceContextGuard&& /*initTraceContextGuard*/) override
    {
        YT_VERIFY(Probe_->RunOneEpoch);
        if (Probe_->CaptureProcessingStatus) {
            THashMap<TStreamId, TInflightStreamTraverseDataPtr> inflights{
                {OutputStreamId, New<TInflightStreamTraverseData>()},
            };
            for (int epoch = 0; epoch < 2; ++epoch) {
                auto iterGuard = StartRunIteration(context);
                if (epoch == 0) {
                    UpdateStatus(TSystemTimestamp(1), TSystemTimestamp(1), inflights);
                }
                auto transaction = PrepareTransaction(context);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                Commit(context, transaction);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                auto reportTime = TSystemTimestamp(epoch + 2);
                UpdateStatus(reportTime, reportTime, inflights);
                Probe_->ProcessingStatuses.push_back(GetStatus());
                auto detachedStatus = GetStatus();
                detachedStatus->NodeTraverse.Reset();
                detachedStatus->ProcessingObservation.Reset();
                Probe_->ProcessingStatuses.push_back(GetStatus());
                FinishRunIteration();
            }
            THROW_ERROR_EXCEPTION("Processing status test completed");
        }
        for (int epoch = 0; epoch < Probe_->EpochCount; ++epoch) {
            auto iterGuard = StartRunIteration(context);
            auto transaction = PrepareTransaction(context);
            Commit(context, transaction);
            FinishRunIteration();
        }
        THROW_ERROR_EXCEPTION("Sink init test stopped after one epoch");
    }

    void ProcessDistributedMessages(
        const IComputationRunContextPtr& /*context*/,
        std::deque<TOutputMessageConstPtr>&& messages) override
    {
        if (Probe_->PersistOutputMessages) {
            OutputStore_->TryUnregisterBatch(messages);
        }
    }

private:
    TSinkInitProbe* const Probe_;
};

YT_FLOW_DEFINE_COMPUTATION(TProductionWiredSinkInitComputation);

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const IClientPtr Client_;
};

struct TSinkDescription
{
    TSinkId SinkId;
    bool AcceptOutput = false;
};

TComputationSpecPtr MakeSpec(const std::vector<TSinkDescription>& sinks)
{
    auto specYson = Format("{computation_class_name=%Qv;output_stream_ids=[%Qv];}",
        TypeName<TProductionWiredSinkInitComputation>(),
        OutputStreamId);
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    for (const auto& sink : sinks) {
        auto sinkSpec = New<TSinkSpec>();
        sinkSpec->SinkClassName = TypeName<TRecordingSink>();
        if (sink.AcceptOutput) {
            sinkSpec->InputStreamIds.insert(OutputStreamId);
        }
        sinkSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
        spec->Sinks.emplace(sink.SinkId, std::move(sinkSpec));
    }
    return spec;
}

TComputationStreamSpecStoragePtr MakeStreamSpecStorage()
{
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = New<TTableSchema>();
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
    specs[OutputStreamId][TStreamSpecId(1)] = streamSpec;
    return New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(specs)),
        New<TTableSchema>(),
        /*converterCache*/ nullptr);
}

TDynamicComputationContextPtr MakeDynamicContext()
{
    auto context = New<TDynamicComputationContext>();
    context->DynamicComputationSpec = ConvertTo<TDynamicComputationSpecPtr>(TYsonStringBuf("{}"));
    context->DynamicPartitionSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf(
        "{computation_partition_spec={};}"));
    return context;
}

TError RunProductionWiredComputation(
    const TComputationSpecPtr& spec,
    std::optional<TKey> sourceKey,
    TSinkInitProbe* probe,
    bool distributeOnPrepare,
    bool runOneEpoch = false,
    TDynamicComputationContextPtr reconfigureBeforeRun = nullptr)
{
    auto queue = New<TActionQueue>("SinkInitTest");
    auto timestampProvider = New<NiceMock<TMockTimestampProvider>>();
    auto nextTimestamp = std::make_shared<ui64>(1ULL << 32);
    ON_CALL(*timestampProvider, GenerateTimestamps(_, _))
        .WillByDefault([nextTimestamp] (auto, auto) {
            return MakeFuture(TTimestamp((*nextTimestamp)++));
        });

    auto client = New<NiceMock<TMockClient>>();
    client->SetTimestampProvider(timestampProvider);
    if (probe->WarmupTransactions) {
        probe->WarmupTransactions->Install(client.Get());
    } else {
        auto transaction = New<NiceMock<TMockTransaction>>();
        transaction->StartTimestamp = TTimestamp(1);
        ON_CALL(*transaction, ModifyRows(_, _, _, _))
            .WillByDefault([probe] (const auto& path, auto, const auto&, auto) {
                if (path == "//pipeline/compact_partition_output_messages" || path == "//pipeline/compact_output_messages") {
                    ++probe->OutputWriteCalls;
                }
            });
        ON_CALL(*transaction, Commit(_))
            .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));
        ON_CALL(*client, StartTransaction(_, _))
            .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));
    }

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("test");
    partition->SourceKey = std::move(sourceKey);
    partition->State = EPartitionState::Executing;

    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->Generation = JobGeneration;
    job->PartitionId = partition->PartitionId;
    job->LeaseId = TLeaseId(TGuid::Create());

    auto streamSpecStorage = MakeStreamSpecStorage();
    TStreamLimitUsageStateMap outputStreamStates{
        {OutputStreamId, New<TStreamLimitUsageState>()},
    };

    auto context = New<TComputationContext>();
    context->ComputationSpec = spec;
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PipelinePath = NYPath::TRichYPath("//pipeline");
    context->PipelinePath.SetCluster("test");
    context->Partition = partition;
    context->Job = job;
    context->SerializedInvoker = queue->GetInvoker();
    context->PoolInvoker = queue->GetInvoker();
    context->Logger = NLogging::TLogger("SinkInitTest");
    context->Profiler = NProfiling::TProfiler();
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->PartitionBufferState = CreateDetachedPartitionBufferState(std::move(outputStreamStates));
    context->StreamSpecStorage = streamSpecStorage;
    context->DistributedThrottlerControllerChannelProvider = [] {
        return NRpc::IChannelPtr{};
    };

    auto dynamicContext = MakeDynamicContext();
    if (const auto& transactions = probe->WarmupTransactions) {
        partition->PartitionId = transactions->PartitionId;
        job->PartitionId = transactions->PartitionId;
        job->LeaseId = transactions->LeaseId;
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("warmup-test", context->Profiler),
            context->Logger,
            context->Profiler);
        context->PartitionBufferState = probe->PartitionBufferState;
        dynamicContext->DynamicComputationSpec->StateManager->Format->Compress = false;
        dynamicContext->DynamicComputationSpec->StateManager->Format->RecodeProbability = 0.0;
    }

    auto runContext = New<TTestComputationRunContext>();
    probe->DistributeOnPrepare = distributeOnPrepare;
    probe->RunOneEpoch = runOneEpoch;
    YT_VERIFY(!GetActiveSinkInitProbe().exchange(probe));
    auto resetProbe = Finally([] {
        GetActiveSinkInitProbe().store(nullptr);
    });
    TIntrusivePtr<TProductionWiredSinkInitComputation> computation;
    auto execution = BIND([
        &computation,
        context = std::move(context),
        dynamicContext = std::move(dynamicContext),
        runContext = std::move(runContext),
        reconfigureBeforeRun = std::move(reconfigureBeforeRun),
        probe
    ] {
        computation = New<TProductionWiredSinkInitComputation>(
            context,
            dynamicContext);
        computation->UpdateWatermarkState(New<TWatermarkState>());
        computation->SetInputTraverse({});
        if (reconfigureBeforeRun) {
            computation->Reconfigure(reconfigureBeforeRun);
        }
        if (probe->ClearBufferStateBeforeRun) {
            context->PartitionBufferState.Reset();
        }
        computation->RunForTest(runContext);
    })
        .AsyncVia(queue->GetInvoker())
        .Run();
    if (probe->CancelDuringPrepare) {
        EXPECT_TRUE(WaitFor(probe->PrepareStarted.ToFuture().WithTimeout(TDuration::Seconds(5))).IsOK());
        execution.Cancel(TError(NYT::EErrorCode::Canceled, "Cancel retained computation"));
    }
    auto result = WaitFor(execution);
    probe->TerminationsBeforeDestruction = std::count_if(probe->Events.begin(), probe->Events.end(), [] (const auto& event) {
        return event.Type == TSinkInitProbe::EEventType::Terminate;
    });
    computation.Reset();
    return result;
}

std::vector<TSinkInitProbe::TEvent> GetEvents(
    const TSinkInitProbe& probe,
    TSinkInitProbe::EEventType type)
{
    std::vector<TSinkInitProbe::TEvent> result;
    for (const auto& event : probe.Events) {
        if (event.Type == type) {
            result.push_back(event);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

MATCHER_P(BitwiseEqualRows, expected, "")
{
    if (arg.size() != expected.size()) {
        return false;
    }
    for (const auto& [key, row] : expected) {
        auto it = arg.find(key);
        if (it == arg.end() || !TBitwiseUnversionedRowEqual()(it->second, row)) {
            return false;
        }
    }
    return true;
}

TPartitionBufferWarmup MakeTestWarmup(double cycleSeconds)
{
    TPartitionBufferWarmup result;
    result.InputSpeeds[TStreamId("input")] = 100.0 * cycleSeconds;
    result.OutputSpeeds[OutputStreamId] = 200.0 * cycleSeconds;
    result.EpochCycleSeconds = cycleSeconds;
    return result;
}

std::shared_ptr<TBufferWarmupTransactionProbe> MakeSeededWarmupTransactions()
{
    auto result = std::make_shared<TBufferWarmupTransactionProbe>();
    result->Seed(result->LegacyKey(), ConvertToYsonString(std::string("corrupt warmup")));
    result->Seed(result->CanonicalKey(), ConvertToYsonString(MakeTestWarmup(1.0)));
    for (const auto* name : {
            "/application",
            "__buffer_warmup/child",
            "__buffer_warmup_extra",
            "/__buffer_warmup/child"})
    {
        result->Seed({result->PartitionId, name}, ConvertToYsonString(std::string(name)));
    }
    result->Seed(
        {result->OtherPartitionId, LegacyBufferWarmupStateName},
        ConvertToYsonString(std::string("other legacy")));
    result->Seed(
        {result->OtherPartitionId, BufferWarmupStateName},
        ConvertToYsonString(MakeTestWarmup(9.0)));
    return result;
}

TError RunWarmupComputation(
    TSinkInitProbe* probe,
    std::shared_ptr<TBufferWarmupTransactionProbe> transactions,
    IPartitionBufferStatePtr buffer)
{
    probe->WarmupTransactions = std::move(transactions);
    probe->PartitionBufferState = std::move(buffer);
    return RunProductionWiredComputation(MakeSpec({}), std::nullopt, probe, false, true);
}

TEST(TBufferWarmupCleanupTest, PreservesCanonicalAndUnrelatedRows)
{
    auto transactions = MakeSeededWarmupTransactions();
    const auto legacy = transactions->LegacyKey();
    auto expected = transactions->Rows;
    expected.erase(legacy);
    auto buffer = New<TRecordingBufferWarmupState>();
    buffer->RefreshPeriod = TDuration::Hours(1);
    TSinkInitProbe probe;

    auto error = RunWarmupComputation(&probe, transactions, buffer);

    ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    EXPECT_THAT(transactions->Rows, BitwiseEqualRows(expected));
    EXPECT_THAT(transactions->StateLookups, ElementsAre(TWarmupStateKey{transactions->PartitionId, "/__buffer_warmup"}));
    EXPECT_THAT(transactions->AttemptedDeletes, ElementsAre(TWarmupStateKey{transactions->PartitionId, "__buffer_warmup"}));
    EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
    ASSERT_EQ(buffer->Seeds.size(), 1u);
    EXPECT_EQ(buffer->Seeds[0].InputSpeeds, MakeTestWarmup(1.0).InputSpeeds);
    EXPECT_EQ(buffer->Seeds[0].OutputSpeeds, MakeTestWarmup(1.0).OutputSpeeds);
    EXPECT_EQ(buffer->Seeds[0].EpochCycleSeconds, 1.0);
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::ContextCommit).size(), 1u);
}

TEST(TBufferWarmupCleanupTest, CleansWithDisabledOrNullWarmup)
{
    for (bool clearBufferState : {false, true}) {
        SCOPED_TRACE(clearBufferState);
        auto transactions = MakeSeededWarmupTransactions();
        const auto legacy = transactions->LegacyKey();
        auto expected = transactions->Rows;
        expected.erase(legacy);
        auto buffer = New<TRecordingBufferWarmupState>();
        buffer->Enabled = false;
        TSinkInitProbe probe;
        probe.ClearBufferStateBeforeRun = clearBufferState;

        auto error = RunWarmupComputation(&probe, transactions, buffer);

        ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
        EXPECT_THAT(transactions->Rows, BitwiseEqualRows(expected));
        EXPECT_TRUE(transactions->StateLookups.empty());
        EXPECT_TRUE(buffer->Seeds.empty());
        EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
        EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::ContextCommit).size(), 1u);
    }
}

TEST(TBufferWarmupCleanupTest, FreshPartitionWritesCanonicalWarmup)
{
    auto transactions = std::make_shared<TBufferWarmupTransactionProbe>();
    const auto legacy = transactions->LegacyKey();
    const auto canonical = transactions->CanonicalKey();
    auto buffer = New<TRecordingBufferWarmupState>();
    buffer->CurrentWarmup = MakeTestWarmup(2.0);
    TSinkInitProbe probe;

    auto error = RunWarmupComputation(&probe, transactions, buffer);

    ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    ASSERT_TRUE(transactions->Rows.contains(canonical));
    EXPECT_FALSE(transactions->Rows.contains(legacy));
    EXPECT_EQ(transactions->Rows.size(), 1u);
    EXPECT_EQ(transactions->ReadState(canonical), ConvertToYsonString(buffer->CurrentWarmup));
    EXPECT_THAT(transactions->StateLookups, ElementsAre(canonical));
    EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
    EXPECT_TRUE(buffer->Seeds.empty());
}

TEST(TBufferWarmupCleanupTest, RefreshesRecoveredCanonicalWarmup)
{
    auto transactions = MakeSeededWarmupTransactions();
    const auto legacy = transactions->LegacyKey();
    const auto canonical = transactions->CanonicalKey();
    auto expectedUnrelated = transactions->Rows;
    expectedUnrelated.erase(legacy);
    expectedUnrelated.erase(canonical);
    auto buffer = New<TRecordingBufferWarmupState>();
    buffer->CurrentWarmup = MakeTestWarmup(3.0);
    TSinkInitProbe probe;

    auto error = RunWarmupComputation(&probe, transactions, buffer);

    ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    ASSERT_TRUE(transactions->Rows.contains(canonical));
    EXPECT_EQ(transactions->ReadState(canonical), ConvertToYsonString(buffer->CurrentWarmup));
    auto actualUnrelated = transactions->Rows;
    actualUnrelated.erase(canonical);
    EXPECT_THAT(actualUnrelated, BitwiseEqualRows(expectedUnrelated));
    ASSERT_EQ(buffer->Seeds.size(), 1u);
    EXPECT_EQ(buffer->Seeds[0].EpochCycleSeconds, 1.0);
    EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
}

TEST(TBufferWarmupCleanupTest, BoundsCleanupPerJobAndRepeatsAfterRestart)
{
    auto transactions = MakeSeededWarmupTransactions();
    const auto legacy = transactions->LegacyKey();
    auto expected = transactions->Rows;
    expected.erase(legacy);
    auto buffer = New<TRecordingBufferWarmupState>();
    buffer->Enabled = false;
    for (int run = 0; run < 3; ++run) {
        SCOPED_TRACE(run);
        if (run == 2) {
            transactions->Seed(legacy, ConvertToYsonString(std::string("recreated by old worker")));
        }
        TSinkInitProbe probe;
        probe.EpochCount = 2;

        auto error = RunWarmupComputation(&probe, transactions, buffer);

        ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
        EXPECT_THAT(transactions->Rows, BitwiseEqualRows(expected));
        EXPECT_EQ(transactions->CommittedDeletes.size(), static_cast<size_t>(run + 1));
        EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::ContextCommit).size(), 2u);
    }
    EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy, legacy, legacy));
    EXPECT_TRUE(transactions->StateLookups.empty());
}

TEST(TBufferWarmupCleanupTest, RetriesFailedAndUncertainCommit)
{
    for (bool commitBeforeError : {false, true}) {
        SCOPED_TRACE(commitBeforeError);
        auto transactions = MakeSeededWarmupTransactions();
        const auto legacy = transactions->LegacyKey();
        auto expected = transactions->Rows;
        expected.erase(legacy);
        transactions->NextCleanupError = TError(NYT::EErrorCode::Timeout, "Injected lost commit reply");
        ASSERT_TRUE(IsFlowRetriableError(transactions->NextCleanupError));
        transactions->CommitBeforeError = commitBeforeError;
        auto buffer = New<TRecordingBufferWarmupState>();
        buffer->Enabled = false;
        TSinkInitProbe probe;

        auto error = RunWarmupComputation(&probe, transactions, buffer);

        ASSERT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
        EXPECT_THAT(transactions->Rows, BitwiseEqualRows(expected));
        EXPECT_EQ(transactions->FailedCleanupCommits, 1);
        EXPECT_EQ(transactions->TimestampLookups, 1);
        EXPECT_EQ(transactions->AttemptedDeletes.size(), commitBeforeError ? 1u : 2u);
        for (const auto& key : transactions->AttemptedDeletes) {
            EXPECT_EQ(key, legacy);
        }
        EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
        EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::ContextCommit).size(), 1u);
    }
}

TEST(TBufferWarmupCleanupTest, CanceledCommitPreservesRowsAndRestartRetries)
{
    auto transactions = MakeSeededWarmupTransactions();
    const auto legacy = transactions->LegacyKey();
    const auto before = transactions->Rows;
    transactions->NextCleanupError = TError(NYT::EErrorCode::Canceled, "Injected cancellation");
    auto buffer = New<TRecordingBufferWarmupState>();
    buffer->Enabled = false;
    TSinkInitProbe failedProbe;

    auto failed = RunWarmupComputation(&failedProbe, transactions, buffer);

    EXPECT_FALSE(failed.IsOK());
    EXPECT_NE(failed.GetMessage(), "Sink init test stopped after one epoch");
    EXPECT_THAT(transactions->Rows, BitwiseEqualRows(before));
    EXPECT_THAT(transactions->AttemptedDeletes, ElementsAre(legacy));
    EXPECT_TRUE(transactions->CommittedDeletes.empty());
    EXPECT_EQ(transactions->FailedCleanupCommits, 1);
    EXPECT_TRUE(GetEvents(failedProbe, TSinkInitProbe::EEventType::ContextCommit).empty());

    TSinkInitProbe restartedProbe;
    auto restarted = RunWarmupComputation(&restartedProbe, transactions, buffer);

    ASSERT_EQ(restarted.GetMessage(), "Sink init test stopped after one epoch");
    auto expected = before;
    expected.erase(legacy);
    EXPECT_THAT(transactions->Rows, BitwiseEqualRows(expected));
    EXPECT_THAT(transactions->CommittedDeletes, ElementsAre(legacy));
    EXPECT_EQ(GetEvents(restartedProbe, TSinkInitProbe::EEventType::ContextCommit).size(), 1u);
}

TEST(TComputationStatusPublicationTest, PublishesCommittedObservationWithTraverse)
{
    TSinkInitProbe probe;
    probe.CaptureProcessingStatus = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Processing status test completed");
    ASSERT_EQ(probe.ProcessingStatuses.size(), 8u);
    const auto& initial = probe.ProcessingStatuses[0];
    ASSERT_TRUE(initial->NodeTraverse);
    EXPECT_EQ(initial->NodeTraverse->ReportTime, TSystemTimestamp(1));
    EXPECT_FALSE(initial->ProcessingObservation);

    for (int epoch = 0; epoch < 2; ++epoch) {
        for (int index = 2; index <= 3; ++index) {
            const auto& published = probe.ProcessingStatuses[4 * epoch + index];
            ASSERT_TRUE(published->NodeTraverse);
            ASSERT_TRUE(published->ProcessingObservation);
            EXPECT_EQ(published->NodeTraverse->ReportTime, TSystemTimestamp(epoch + 2));
            EXPECT_EQ(published->NodeTraverse->IterationCycle, epoch);
            EXPECT_EQ(published->ProcessingObservation->Sequence, epoch + 1);
        }
    }
    for (int epoch = 0; epoch < 2; ++epoch) {
        const auto& committed = probe.ProcessingStatuses[4 * epoch + 1];
        ASSERT_TRUE(committed->ProcessingObservation);
        EXPECT_EQ(committed->ProcessingObservation->Sequence, epoch + 1);
        EXPECT_EQ(committed->NodeTraverse, probe.ProcessingStatuses[4 * epoch]->NodeTraverse);
    }
    const auto& pending = probe.ProcessingStatuses[4];
    EXPECT_EQ(pending->NodeTraverse, probe.ProcessingStatuses[2]->NodeTraverse);
    EXPECT_EQ(pending->ProcessingObservation, probe.ProcessingStatuses[2]->ProcessingObservation);
}

TEST(TSinkInitTest, RunsPartitionSinkThroughFirstEpoch)
{
    using enum TSinkInitProbe::EEventType;

    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    std::vector<TSinkInitProbe::EEventType> eventTypes;
    for (const auto& event : probe.Events) {
        eventTypes.push_back(event.Type);
    }
    EXPECT_THAT(eventTypes, ElementsAre(Construct, Watermark, Init, Prepare, Distribute, Watermark, PreSyncFlush, Sync, ContextCommit, Commit, Terminate));
    EXPECT_EQ(probe.TerminationsBeforeDestruction, 1);
    const auto constructions = GetEvents(probe, Construct);
    ASSERT_EQ(constructions.size(), 1u);
    for (const auto& event : probe.Events) {
        if (event.Instance) {
            EXPECT_EQ(event.Instance, constructions[0].Instance);
            EXPECT_EQ(event.Generation, JobGeneration);
        }
    }
}

TEST(TSinkInitTest, PreSyncFlushAcknowledgedMessagesAreNotWrittenToOutputStore)
{
    TSinkInitProbe probe;
    probe.PersistOutputMessages = true;
    probe.AckOnDistribute = false;
    probe.AckInPreSyncFlush = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("prepared"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Distribute).size(), 1u);
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Commit).size(), 1u);
    EXPECT_EQ(probe.OutputWriteCalls, 0);
}

TEST(TSinkInitTest, PreSyncFlushUnacknowledgedMessagesAreWrittenToOutputStore)
{
    TSinkInitProbe probe;
    probe.PersistOutputMessages = true;
    probe.AckOnDistribute = false;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("prepared"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Distribute).size(), 1u);
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Commit).size(), 1u);
    EXPECT_GT(probe.OutputWriteCalls, 0);
}

TEST(TSinkInitTest, FlushesAllSinksBeforeAnySync)
{
    using enum TSinkInitProbe::EEventType;

    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("used"), true}, {TSinkId("unused"), false}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    std::vector<TSinkInitProbe::EEventType> phases;
    for (const auto& event : probe.Events) {
        if (event.Type == PreSyncFlush || event.Type == Sync || event.Type == ContextCommit || event.Type == Commit) {
            phases.push_back(event.Type);
        }
    }
    EXPECT_THAT(phases, ElementsAre(PreSyncFlush, PreSyncFlush, Sync, Sync, ContextCommit, Commit, Commit));
}

TEST(TSinkInitTest, FailedPreSyncFlushPreventsSyncAndCommit)
{
    using enum TSinkInitProbe::EEventType;

    TSinkInitProbe probe;
    probe.FailPreSyncFlush = true;
    probe.PersistOutputMessages = true;
    probe.AckOnDistribute = false;
    probe.AckInPreSyncFlush = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("prepared"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Recording sink pre-sync flush failed");
    EXPECT_EQ(GetEvents(probe, PreSyncFlush).size(), 1u);
    EXPECT_TRUE(GetEvents(probe, Sync).empty());
    EXPECT_TRUE(GetEvents(probe, ContextCommit).empty());
    EXPECT_TRUE(GetEvents(probe, Commit).empty());
    EXPECT_EQ(probe.OutputWriteCalls, 0);
}

TEST(TSinkInitTest, AppliesPendingDynamicSpecBeforeSinkConstruction)
{
    const TSinkId sinkId("ordinary");
    auto reconfiguredContext = MakeDynamicContext();
    reconfiguredContext->SpecGeneration = 1;
    auto reconfiguredSinkSpec = New<TDynamicSinkSpec>();
    reconfiguredSinkSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    reconfiguredContext->DynamicComputationSpec->Sinks.emplace(sinkId, reconfiguredSinkSpec);

    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({{sinkId, false}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false,
        /*runOneEpoch*/ false,
        std::move(reconfiguredContext));

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after DoPrepare");
    const auto constructions = GetEvents(probe, TSinkInitProbe::EEventType::Construct);
    ASSERT_EQ(constructions.size(), 1u);
    EXPECT_EQ(constructions[0].DynamicSinkSpec, reconfiguredSinkSpec);
}

TEST(TSinkInitTest, InitializesEveryKeyedSinkBeforePrepareAndReusesMatchingSink)
{
    TSinkInitProbe probe;
    auto error = RunProductionWiredComputation(
        MakeSpec({
            {TSinkId("ordinary"), true},
            {TSinkId("unused"), false},
        }),
        MakeKey("source-key"),
        &probe,
        /*distributeOnPrepare*/ true,
        /*runOneEpoch*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink init test stopped after one epoch");
    const auto constructions = GetEvents(probe, TSinkInitProbe::EEventType::Construct);
    const auto inits = GetEvents(probe, TSinkInitProbe::EEventType::Init);
    const auto prepares = GetEvents(probe, TSinkInitProbe::EEventType::Prepare);
    const auto distributions = GetEvents(probe, TSinkInitProbe::EEventType::Distribute);
    const auto syncs = GetEvents(probe, TSinkInitProbe::EEventType::Sync);
    const auto commits = GetEvents(probe, TSinkInitProbe::EEventType::Commit);
    ASSERT_EQ(constructions.size(), 2u);
    for (const auto& construction : constructions) {
        EXPECT_EQ(construction.Generation, JobGeneration);
    }
    ASSERT_EQ(inits.size(), 2u);
    ASSERT_EQ(prepares.size(), 1u);
    ASSERT_EQ(distributions.size(), 1u);
    ASSERT_EQ(syncs.size(), 2u);
    ASSERT_EQ(commits.size(), 2u);
    const auto terminations = GetEvents(probe, TSinkInitProbe::EEventType::Terminate);
    ASSERT_EQ(terminations.size(), 2u);
    for (const auto& construction : constructions) {
        EXPECT_EQ(
            std::count_if(terminations.begin(), terminations.end(), [&] (const auto& termination) {
                return termination.Instance == construction.Instance;
            }),
            1);
    }
    EXPECT_EQ(probe.TerminationsBeforeDestruction, 2);

    auto prepare = std::find_if(probe.Events.begin(), probe.Events.end(), [] (const auto& event) {
        return event.Type == TSinkInitProbe::EEventType::Prepare;
    });
    ASSERT_NE(prepare, probe.Events.end());
    EXPECT_EQ(std::count_if(probe.Events.begin(), prepare, [] (const auto& event) {
        return event.Type == TSinkInitProbe::EEventType::Init;
    }),
        2);

    auto matchingConstruction = std::find_if(constructions.begin(), constructions.end(), [&] (const auto& event) {
        return event.SinkId == distributions[0].SinkId;
    });
    ASSERT_NE(matchingConstruction, constructions.end());
    EXPECT_EQ(matchingConstruction->Instance, distributions[0].Instance);
}

TEST(TSinkInitTest, InitFailurePreventsPrepare)
{
    TSinkInitProbe probe;
    probe.FailInit = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("unused"), false}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false);

    EXPECT_EQ(error.GetMessage(), "Recording sink init failed");
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Init).size(), 1u);
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Terminate).size(), 1u);
    EXPECT_EQ(probe.TerminationsBeforeDestruction, 1);
    EXPECT_TRUE(GetEvents(probe, TSinkInitProbe::EEventType::Prepare).empty());
}

TEST(TSinkInitTest, TerminatesSinksWhenCanceledComputationRemainsOwned)
{
    TSinkInitProbe probe;
    probe.CancelDuringPrepare = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        std::nullopt,
        &probe,
        /*distributeOnPrepare*/ false);

    EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled));
    EXPECT_EQ(probe.TerminationsBeforeDestruction, 1);
    EXPECT_EQ(GetEvents(probe, TSinkInitProbe::EEventType::Terminate).size(), 1u);
}

TEST(TSinkInitTest, RejectsMismatchingParentKeyBeforeDistribution)
{
    TSinkInitProbe probe;
    probe.MismatchedParentKey = true;
    auto error = RunProductionWiredComputation(
        MakeSpec({{TSinkId("ordinary"), true}}),
        MakeKey("source-key"),
        &probe,
        /*distributeOnPrepare*/ true);

    EXPECT_EQ(error.GetMessage(), "Sink parent key does not match partition source key");
    EXPECT_TRUE(GetEvents(probe, TSinkInitProbe::EEventType::Distribute).empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
