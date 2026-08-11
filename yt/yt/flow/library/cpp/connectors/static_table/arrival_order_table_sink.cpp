#include "arrival_order_table_sink.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/api/table_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cypress_client/public.h>
#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>

namespace NYT::NFlow::NStaticTableConnector {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TArrivalOrderTableSinkController::GetReceiverChannelCount()
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TArrivalOrderTableSink::TArrivalOrderTableSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , RetryErrorState_(GetContext()->StatusProfiler->ErrorState("/retry"))
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Sink %Qv expects exactly one input stream but got %v",
            TypeName(*this),
            GetSpec()->InputStreamIds.size());
    }

    const auto& inputStreamId = *GetSpec()->InputStreamIds.begin();
    Schema_ = GetContext()->StreamSpecStorage->GetSchema(inputStreamId);
    if (const auto& columnName = GetParameters()->DataWeightColumn) {
        const auto* dataWeightColumn = Schema_->FindColumn(*columnName);
        THROW_ERROR_EXCEPTION_UNLESS(dataWeightColumn,
            "Data weight column %Qv is missing from input stream schema",
            *columnName);
        THROW_ERROR_EXCEPTION_UNLESS(
            dataWeightColumn->GetWireType() == EValueType::Int64 || dataWeightColumn->GetWireType() == EValueType::Uint64,
            "Data weight column %Qv must have int64 or uint64 wire type",
            *columnName);
        DataWeightColumnId_ = Schema_->GetColumnIndex(*dataWeightColumn);
    }
}

TInstant TArrivalOrderTableSink::GetNextTableTimestamp(TInstant now, TDuration tablePeriod)
{
    const auto period = tablePeriod.MicroSeconds();
    return TInstant::MicroSeconds((now.MicroSeconds() / period + 1) * period);
}

TArrivalOrderTableSinkPartitionProgressPtr TArrivalOrderTableSink::GetPartitionProgress(
    const TArrivalOrderTableSinkProgressPtr& progress) const
{
    const auto it = progress->Partitions.find(PartitionKey_);
    return it == progress->Partitions.end() ? nullptr : it->second;
}

std::optional<TMessageId> TArrivalOrderTableSink::GetMessageId(
    const TArrivalOrderTableSinkPartitionProgressPtr& partitionProgress)
{
    return partitionProgress ? std::optional(partitionProgress->MessageId) : std::nullopt;
}

void TArrivalOrderTableSinkOwner::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_path", &TThis::PipelinePath)
        .Default();
    registrar.Parameter("computation_id", &TThis::ComputationId)
        .Default();
    registrar.Parameter("sink_id", &TThis::SinkId)
        .Default();
    // Compared via YsonStruct equality in #ValidateProgressOwnership(): adding a field here
    // would change the identity of every existing directory, so the struct is frozen.
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
}

void TArrivalOrderTableSinkPartitionProgress::Register(TRegistrar registrar)
{
    registrar.Parameter("system_timestamp", &TThis::SystemTimestamp)
        .Default();
    registrar.Parameter("message_id", &TThis::MessageId)
        .Default();
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
}

void TArrivalOrderTableSinkProgress::Register(TRegistrar registrar)
{
    registrar.Parameter("owner", &TThis::Owner)
        .DefaultNew();
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
    registrar.Parameter("next_table_timestamp", &TThis::NextTableTimestamp)
        .Default();
    // The attribute is rewritten wholesale on every commit; an older binary in a mixed fleet
    // must not strip fields a newer one has written.
    registrar.UnrecognizedStrategy(NYTree::EUnrecognizedStrategy::KeepRecursive);
}

////////////////////////////////////////////////////////////////////////////////

TArrivalOrderTableSinkOwnerPtr TArrivalOrderTableSink::GetOwner() const
{
    auto owner = New<TArrivalOrderTableSinkOwner>();
    owner->PipelinePath = GetContext()->PipelinePath;
    owner->ComputationId = GetContext()->Partition->ComputationId;
    owner->SinkId = GetContext()->SinkId;
    return owner;
}

void TArrivalOrderTableSink::ValidateProgressOwnership(const TArrivalOrderTableSinkProgressPtr& progress) const
{
    auto owner = GetOwner();
    THROW_ERROR_EXCEPTION_UNLESS(*progress->Owner == *owner,
        "Output directory %v already holds the progress of another writer; "
        "remove its @progress attribute to hand the directory over",
        OutputDirectory_)
        .With("stored_owner", progress->Owner)
        .With("expected_owner", owner);
}

TArrivalOrderTableSinkProgressPtr TArrivalOrderTableSink::ReadOrSeedProgress(
    const NApi::ITransactionPtr& transaction) const
{
    // The directory may predate the sink, and #CreateNode with |IgnoreExisting| leaves such a
    // directory untouched, so the attribute has to be seeded here rather than at creation.
    auto progressOrError = WaitFor(transaction->GetNode(OutputDirectory_ + "/@progress"));
    if (!progressOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return ConvertTo<TArrivalOrderTableSinkProgressPtr>(progressOrError.ValueOrThrow());
    }

    auto progress = New<TArrivalOrderTableSinkProgress>();
    progress->Owner = GetOwner();
    progress->NextTableTimestamp = GetNextTableTimestamp(TInstant::Now(), GetParameters()->TablePeriod);

    // A handed-over directory may hold tables for slots ahead of the wall clock; the grid has to
    // continue past them, or the first colliding #CreateNode would be retried forever.
    TListNodeOptions listOptions;
    listOptions.Attributes = {"table_timestamp"};
    auto tables = ConvertTo<NYTree::IListNodePtr>(
        WaitFor(transaction->ListNode(OutputDirectory_, listOptions)).ValueOrThrow());
    for (const auto& table : tables->GetChildren()) {
        const auto tableTimestamp = table->Attributes().Find<TInstant>("table_timestamp");
        THROW_ERROR_EXCEPTION_UNLESS(tableTimestamp,
            "Output directory %v cannot be adopted: child %Qv has no table_timestamp attribute; "
            "remove foreign children or use a fresh directory",
            OutputDirectory_,
            table->AsString()->GetValue());
        progress->NextTableTimestamp = std::max(
            progress->NextTableTimestamp,
            *tableTimestamp + GetParameters()->TablePeriod);
    }

    WaitFor(transaction->SetNode(
        OutputDirectory_ + "/@progress",
        NYson::ConvertToYsonString(progress)))
        .ThrowOnError();
    return progress;
}

void TArrivalOrderTableSink::LockProgress(const NApi::ITransactionPtr& transaction) const
{
    // Keyed shared lock: progress writers exclude each other without blocking
    // the creation of output tables in the same directory.
    TLockNodeOptions options;
    options.AttributeKey = "progress";
    options.Waitable = true;
    const auto result = WaitFor(transaction->LockNode(OutputDirectory_, NCypressClient::ELockMode::Shared, options))
        .ValueOrThrow();
    // A conflicting waitable lock is parked as pending and the call returns at once, so the
    // acquisition has to be awaited explicitly.
    while (true) {
        const auto state = ConvertTo<std::string>(
            WaitFor(transaction->GetNode(Format("#%v/@state", result.LockId))).ValueOrThrow());
        if (state == "acquired") {
            return;
        }
        TDelayedExecutor::WaitForDuration(GetDynamicParameters()->RetryBackoff);
    }
}

void TArrivalOrderTableSink::UpdateWatermarkState(TWatermarkStatePtr state)
{
    WatermarkState_ = std::move(state);
}

TSystemTimestamp TArrivalOrderTableSink::GetSystemWatermark() const
{
    return WatermarkState_
        ? WatermarkState_->GetSystemWatermark(*GetSpec()->InputStreamIds.begin())
        : ZeroSystemTimestamp;
}

bool TArrivalOrderTableSink::IsActiveBatchFull() const
{
    // Sealing needs the slot timestamp, which is only known once the external state is read.
    if (!Initialized_) {
        return false;
    }
    const auto dynamicParameters = GetDynamicParameters();
    return std::ssize(ActiveRequests_) >= dynamicParameters->MaxRowCount ||
        ActiveDataWeight_ >= dynamicParameters->MaxDataWeight;
}

template <class TCallback>
TArrivalOrderTableSinkProgressPtr TArrivalOrderTableSink::RunWithRetries(TCallback&& callback, TStringBuf operation)
{
    for (i64 attempt = 0;; ++attempt) {
        try {
            auto result = callback();
            RetryErrorState_->ClearError();
            return result;
        } catch (const std::exception& ex) {
            auto error = TError(ex);
            if (error.FindMatching(NYT::EErrorCode::Canceled)) {
                throw;
            }
            const auto backoff = GetDynamicParameters()->RetryBackoff;
            RetryErrorState_->SetError(error);
            YT_TLOG_WARNING("Retrying failed attempt")
                .With("Operation", operation)
                .With("Attempt", attempt)
                .With("RetryBackoff", backoff)
                .With(error);
            TDelayedExecutor::WaitForDuration(backoff);
        }
    }
}

void TArrivalOrderTableSink::Init(IInitContextPtr /*initContext*/)
{
    const auto& partition = GetContext()->Partition;
    // The key identifies a partition in persisted state, so it must be injective; #ToString() is
    // not: it truncates long string values.
    PartitionKey_ = partition->SourceKey
        ? std::string(ConvertToYsonString(partition->SourceKey->Underlying(), NYson::EYsonFormat::Text).ToString())
        : std::string(ToString(partition->PartitionId));

    const auto& parameters = GetParameters();
    OutputDirectory_ = parameters->OutputDirectory.GetPath();

    const auto cluster = parameters->OutputDirectory.GetCluster();
    Client_ = cluster
        ? GetContext()->ClientsCache->GetClient(*cluster)
        : GetContext()->GetClient();
}

void TArrivalOrderTableSink::EnsureInitialized()
{
    if (Initialized_) {
        return;
    }
    // Deliberately not done in #Init(): that runs from #DoPrepare() when a restarted job replays its
    // output store, and blocking there would leave the partition without traverse data, freezing
    // the watermark of the whole pipeline. #Commit() runs after the epoch is already durable.
    auto progress = InitializeExternalState();
    // Not retried: a foreign owner never resolves itself; the operator has to hand the directory over.
    ValidateProgressOwnership(progress);
    PersistedThroughMessageId_ = GetMessageId(GetPartitionProgress(progress));
    NextTableTimestamp_ = progress->NextTableTimestamp;
    Initialized_ = true;

    // Requests buffered before initialization go through the regular routing: replayed messages
    // that the stored frontier covers are acknowledged instead of burning slots; the rest obey
    // the slot and size-limit logic.
    auto buffered = std::exchange(ActiveRequests_, {});
    ActiveDataWeight_ = 0;
    for (auto& request : buffered) {
        RouteRequest(std::move(request));
    }
}

i64 TArrivalOrderTableSink::GetDataWeight(const TOutputMessageConstPtr& message) const
{
    if (!DataWeightColumnId_) {
        return message->ByteSize;
    }
    const auto value = GetColumn(*message, *DataWeightColumnId_);
    i64 dataWeight;
    if (value.Type == EValueType::Null) {
        return message->ByteSize;
    } else if (value.Type == EValueType::Int64) {
        dataWeight = value.Data.Int64;
    } else if (value.Type == EValueType::Uint64 && value.Data.Uint64 <= std::numeric_limits<i64>::max()) {
        dataWeight = static_cast<i64>(value.Data.Uint64);
    } else {
        THROW_ERROR_EXCEPTION("Data weight column %Qv must contain a non-negative integer",
            *GetParameters()->DataWeightColumn);
    }
    THROW_ERROR_EXCEPTION_IF(dataWeight < 0,
        "Data weight must be non-negative");
    return dataWeight;
}

void TArrivalOrderTableSink::Distribute(
    const TOutputMessageConstPtr& message,
    TOnDistributedCallback onDistributed)
{
    TRequest request{
        .Message = message,
        .Callback = std::move(onDistributed),
        .DataWeight = GetDataWeight(message),
    };

    RouteRequest(std::move(request));
}

void TArrivalOrderTableSink::RouteRequest(TRequest request)
{
    if (PersistedThroughMessageId_ && request.Message->MessageId <= *PersistedThroughMessageId_) {
        ObserveEventLag(request.Message->StreamId, request.Message->EventTimestamp);
        request.Callback();
        return;
    }
    if (ActiveRequests_.empty() && IsEmptySlotReady(TInstant::Now())) {
        DeferredRequests_.push_back(std::move(request));
    } else {
        AddRequest(std::move(request));
    }
}

void TArrivalOrderTableSink::AddRequest(TRequest request)
{
    YT_VERIFY(request.DataWeight <= std::numeric_limits<i64>::max() - ActiveDataWeight_);
    ActiveDataWeight_ += request.DataWeight;
    ActiveRequests_.push_back(std::move(request));

    if (IsActiveBatchFull()) {
        SealActiveBatch();
    }
}

bool TArrivalOrderTableSink::IsEmptySlotReady(TInstant now) const
{
    if (!Initialized_) {
        return false;
    }
    const auto slotEnd = NextTableTimestamp_ + GetParameters()->TablePeriod;
    return NextTableTimestamp_ <= now &&
        GetSystemWatermark() > TSystemTimestamp(slotEnd.Seconds());
}

void TArrivalOrderTableSink::AssignDeferredRequests(TInstant now)
{
    if (IsEmptySlotReady(now)) {
        return;
    }
    while (!DeferredRequests_.empty()) {
        auto request = std::move(DeferredRequests_.front());
        DeferredRequests_.pop_front();
        AddRequest(std::move(request));
    }
}

void TArrivalOrderTableSink::SealActiveBatch()
{
    auto batch = New<TBatch>();
    batch->TableTimestamp = NextTableTimestamp_;
    batch->Requests = std::exchange(ActiveRequests_, {});
    ReadyBatches_.push_back(std::move(batch));
    NextTableTimestamp_ += GetParameters()->TablePeriod;
    ActiveDataWeight_ = 0;
}

void TArrivalOrderTableSink::Sync(IDynamicTableTransactionPtr /*transaction*/)
{
    // Initialization is driven from #Commit() only: it blocks on a remote master, and #Sync() runs
    // before the epoch transaction is committed, so blocking here would stop the epoch from
    // persisting anything at all.
    if (!Initialized_) {
        return;
    }
    if (InFlightProgress_) {
        // An absent entry means the frontier fell below the watermark and was collected; the
        // retained value is still durably covered, so it is kept rather than reset.
        if (auto messageId = GetMessageId(GetPartitionProgress(InFlightProgress_))) {
            PersistedThroughMessageId_ = messageId;
        }
        NextTableTimestamp_ = std::max(NextTableTimestamp_, InFlightProgress_->NextTableTimestamp);
        InFlightProgress_.Reset();
    }

    const auto now = TInstant::Now();
    const auto batchReady = ActiveRequests_.empty()
        ? IsEmptySlotReady(now)
        : NextTableTimestamp_ <= now || IsActiveBatchFull();
    if (batchReady) {
        SealActiveBatch();
    }
    AssignDeferredRequests(now);
}

void TArrivalOrderTableSink::Commit()
{
    EnsureInitialized();
    std::vector<TRequest> completedRequests;
    if (InFlightBatch_ && !InFlightProgress_) {
        completedRequests = std::move(InFlightBatch_->Requests);
        InFlightBatch_.Reset();
    }
    for (auto& request : completedRequests) {
        ObserveEventLag(request.Message->StreamId, request.Message->EventTimestamp);
        request.Callback();
    }

    if (!InFlightBatch_ && !ReadyBatches_.empty()) {
        // The members are assigned only after the commit succeeds: an exception must not leave
        // the state that reads as "the previous batch is durable, fire its callbacks".
        auto batch = ReadyBatches_.front();
        auto progress = RunWithRetries(
            [&, dynamicParameters = GetDynamicParameters(), systemWatermark = GetSystemWatermark()] {
                return CommitBatchOnce(batch, dynamicParameters, systemWatermark);
            },
            "Arrival order sink batch commit");
        ReadyBatches_.pop_front();
        InFlightBatch_ = std::move(batch);
        InFlightProgress_ = std::move(progress);
    }
}

TArrivalOrderTableSinkProgressPtr TArrivalOrderTableSink::InitializeExternalState()
{
    const auto dynamicParameters = GetDynamicParameters();
    return RunWithRetries(
        [&] {
            auto transaction = StartTransaction(dynamicParameters);
            bool committed = false;
            auto abortGuard = Finally([&] {
                if (!committed) {
                    YT_UNUSED_FUTURE(transaction->Abort());
                }
            });

            TCreateNodeOptions directoryOptions;
            directoryOptions.Recursive = true;
            directoryOptions.IgnoreExisting = true;
            WaitFor(transaction->CreateNode(OutputDirectory_, NObjectClient::EObjectType::MapNode, directoryOptions))
                .ThrowOnError();

            LockProgress(transaction);
            auto progress = ReadOrSeedProgress(transaction);
            WaitFor(transaction->Commit()).ThrowOnError();
            committed = true;
            return progress;
        },
        "Arrival order sink state initialization");
}

////////////////////////////////////////////////////////////////////////////////

ITransactionPtr TArrivalOrderTableSink::StartTransaction(
    const TDynamicArrivalOrderTableSinkParametersPtr& dynamicParameters) const
{
    TTransactionStartOptions startOptions;
    startOptions.Timeout = dynamicParameters->TransactionTimeout;
    return WaitFor(Client_->StartTransaction(
        NTransactionClient::ETransactionType::Master,
        startOptions))
        .ValueOrThrow();
}

TArrivalOrderTableSinkProgressPtr TArrivalOrderTableSink::CommitBatchOnce(
    const TBatchPtr& batch,
    const TDynamicArrivalOrderTableSinkParametersPtr& dynamicParameters,
    TSystemTimestamp systemWatermark)
{
    auto transaction = StartTransaction(dynamicParameters);
    bool committed = false;
    auto abortGuard = Finally([&] {
        if (!committed) {
            YT_UNUSED_FUTURE(transaction->Abort());
        }
    });

    LockProgress(transaction);
    // Seeding here as well lets the sink recover in place when its progress attribute is
    // removed while the job is running.
    auto progress = ReadOrSeedProgress(transaction);
    ValidateProgressOwnership(progress);
    const auto currentProgress = GetPartitionProgress(progress);
    const auto persistedThroughMessageId = GetMessageId(currentProgress);

    const auto batchCovered = !batch->Requests.empty() && std::ranges::all_of(batch->Requests, [&] (const auto& request) {
        return persistedThroughMessageId && request.Message->MessageId <= *persistedThroughMessageId;
    });
    if (batchCovered || (batch->Requests.empty() && progress->NextTableTimestamp > batch->TableTimestamp)) {
        // The read may have seeded the progress attribute; committing keeps Cypress in step
        // with the in-memory state the caller adopts from the returned progress.
        WaitFor(transaction->Commit()).ThrowOnError();
        committed = true;
        return progress;
    }

    std::vector<TUnversionedRow> rows;
    std::optional<TMessageId> maxMessageId = persistedThroughMessageId;
    // Seeded from the stored entry: an empty batch must not reset the frontier of this
    // partition, or another partition's garbage collection would mistake it for a retired one.
    auto maxSystemTimestamp = currentProgress ? currentProgress->SystemTimestamp : ZeroSystemTimestamp;
    for (const auto& request : batch->Requests) {
        maxMessageId = maxMessageId
            ? std::max(*maxMessageId, request.Message->MessageId)
            : std::optional(request.Message->MessageId);
        maxSystemTimestamp = std::max(maxSystemTimestamp, request.Message->SystemTimestamp);
        if (persistedThroughMessageId && request.Message->MessageId <= *persistedThroughMessageId)
        {
            continue;
        }
        rows.push_back(request.Message->Payload.Underlying().Get());
    }

    const auto parameters = GetParameters();
    const auto tableTimestamp = progress->NextTableTimestamp;
    const auto tablePath = Format("%v/%v",
        OutputDirectory_,
        tableTimestamp.FormatGmTime(parameters->TableNameFormat.c_str()));
    TCreateNodeOptions createOptions;
    createOptions.Attributes = CreateEphemeralAttributes();
    createOptions.Attributes->Set("schema", Schema_);
    createOptions.Attributes->Set("table_timestamp", tableTimestamp);
    // Clamped: a catch-up slot may lie further in the past than the TTL, and a table born
    // already expired would be removed right after the commit, losing acknowledged rows.
    createOptions.Attributes->Set(
        "expiration_time",
        std::max(tableTimestamp, TInstant::Now()) + parameters->TableTtl);
    WaitFor(transaction->CreateNode(tablePath, NObjectClient::EObjectType::Table, createOptions))
        .ThrowOnError();

    if (!rows.empty()) {
        auto writer = WaitFor(transaction->CreateTableWriter(tablePath))
            .ValueOrThrow();
        for (int index = 0; const auto& column : Schema_->Columns()) {
            YT_VERIFY(writer->GetNameTable()->GetIdOrRegisterName(column.Name()) == index++);
        }
        // The batch owns the payloads until the delivery callbacks fire, well past this write.
        auto sharedRows = MakeSharedRange(std::move(rows), batch);
        auto ready = writer->Write(sharedRows)
            ? OKFuture
            : writer->GetReadyEvent();
        WaitFor(ready).ThrowOnError();
        WaitFor(writer->Close()).ThrowOnError();
    }

    if (maxMessageId) {
        auto partitionProgress = New<TArrivalOrderTableSinkPartitionProgress>();
        partitionProgress->SystemTimestamp = maxSystemTimestamp;
        partitionProgress->MessageId = *maxMessageId;
        progress->Partitions[PartitionKey_] = std::move(partitionProgress);
    }
    // A partition below the watermark, this one included, has durably drained everything it
    // produced, so its frontier can no longer be needed. This holds because acknowledged messages
    // are durably erased from the output store strictly before the traverse publication that drops
    // them from the inflight set, and the published watermark is capped by the inflight minimum.
    EraseNodesIf(progress->Partitions, [&] (const auto& item) {
        const auto& [partitionKey, partitionProgress] = item;
        return partitionProgress->SystemTimestamp < systemWatermark;
    });
    progress->NextTableTimestamp = tableTimestamp + parameters->TablePeriod;
    WaitFor(transaction->SetNode(
        OutputDirectory_ + "/@progress",
        NYson::ConvertToYsonString(progress)))
        .ThrowOnError();

    WaitFor(transaction->Commit()).ThrowOnError();
    committed = true;
    return progress;
}

DEFINE_REFCOUNTED_TYPE(TArrivalOrderTableSink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
