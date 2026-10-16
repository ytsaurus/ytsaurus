#include "sink.h"

#include "retrying_writer.h"

#include <yt/yt/flow/library/cpp/connectors/common/sync_replica.h>

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <algorithm>
#include <limits>

namespace NYT::NFlow::NSortedDynamicTable {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TMessageContainer>
TSharedRange<NApi::TRowModification> PackRowModifications(
    const TMessageContainer& messages,
    const NTableClient::TNameTablePtr& nameTable,
    const std::optional<THashSet<std::string>>& aggregateColumns,
    bool deleteRows)
{
    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NApi::TRowModification> modifications;
    for (const auto& message : messages) {
        NTableClient::TUnversionedRowBuilder builder(message->PayloadSchema->GetColumnCount() + 1);
        for (int i = 0; i < message->Payload.Underlying().GetCount(); ++i) {
            const auto& column = message->PayloadSchema->Columns()[i];
            if (auto id = nameTable->FindId(column.Name())) {
                auto value = message->Payload.Underlying()[i];
                value.Id = *id;
                if (aggregateColumns && aggregateColumns->contains(column.Name())) {
                    value.Flags = NTableClient::EValueFlags::Aggregate;
                }
                builder.AddValue(value);
            }
        }
        auto row = buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true);
        if (deleteRows) {
            modifications.push_back(NApi::NRowModifications::TDeleteRow(row));
        } else {
            modifications.push_back(NApi::NRowModifications::TWriteRow(row));
        }
    }
    return MakeSharedRange(std::move(modifications), std::move(buffer));
}

template <class TContextPtr, class TSpecPtr, class TParametersPtr>
NTableClient::TNameTablePtr GenerateNameTable(TContextPtr context, TSpecPtr spec, TParametersPtr parameters)
{
    if (spec->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected exactly one input stream id, but got %v",
            spec->InputStreamIds.size());
    }
    auto streamId = *spec->InputStreamIds.begin();
    auto schema = context->StreamSpecStorage->GetSchema(streamId);

    NTableClient::TNameTablePtr nameTable;
    if (parameters->ColumnFilter) {
        nameTable = New<NTableClient::TNameTable>();
        for (const auto& field : *parameters->ColumnFilter) {
            if (schema->FindColumn(field) == nullptr) {
                THROW_ERROR_EXCEPTION("Column %v not found in schema", field);
            }
            nameTable->RegisterNameOrThrow(field);
        }
    } else {
        nameTable = NTableClient::TNameTable::FromSchema(*schema);
    }
    nameTable->RegisterNameOrThrow(NTableClient::SequenceNumberColumnName);
    return nameTable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TInfoControllerState);

void TInfoControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("cached_partition_count", &TThis::CachedPartitionCount)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TSyncSink::TSyncSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TSyncSinkBase::Logger.WithTag("TablePath", GetParameters()->TablePath))
    , NameTable_(GenerateNameTable(GetContext(), GetSpec(), GetParameters()))
{ }

void TSyncSink::DoInit()
{ }

void TSyncSink::DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages)
{
    YT_TLOG_INFO("Synchronously modifying rows in table")
        .With("MessagesCount", std::ssize(messages))
        .With("AggregateColumns", GetParameters()->AggregateColumns)
        .With("DeleteRows", GetParameters()->DeleteRows);
    auto modifications = PackRowModifications(
        messages,
        NameTable_,
        GetParameters()->AggregateColumns,
        GetParameters()->DeleteRows);

    NYT::NApi::TModifyRowsOptions options;
    options.RequireSyncReplica = GetParameters()->RequireSyncReplica;
    transaction->ModifyRows(GetParameters()->TablePath.GetPath(), NameTable_, std::move(modifications), options);
}

DEFINE_REFCOUNTED_TYPE(TSyncSink);

////////////////////////////////////////////////////////////////////////////////

TAsyncSink::TAsyncSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TOrderedBatchingAsyncSinkBase::Logger.WithTag("TablePath", GetParameters()->TablePath))
    , Client_(GetParameters()->TablePath.GetCluster()
            ? GetContext()->ClientsCache->GetClient(*GetParameters()->TablePath.GetCluster())
            : GetContext()->GetClient())
    , NameTable_(GenerateNameTable(GetContext(), GetSpec(), GetParameters()))
    , WriteErrorState_(GetContext()->StatusProfiler->ErrorState("/async_write"))
    , WriteSemaphore_(New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 1))
{ }

void TAsyncSink::DoInit(const std::string& /*producerId*/)
{ }

bool TAsyncSink::TryWriteBatch(const std::vector<TOutputMessageConstPtr>& messages)
{
    try {
        YT_TLOG_INFO("Asynchronously modifying rows in table")
            .With("MessagesCount", std::ssize(messages))
            .With("DeleteRows", GetParameters()->DeleteRows);

        auto transaction = NConcurrency::WaitFor(
            Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        auto modifications = PackRowModifications(
            messages,
            NameTable_,
            /*aggregateColumns*/ std::nullopt,
            GetParameters()->DeleteRows);

        NApi::TModifyRowsOptions options;
        options.RequireSyncReplica = GetParameters()->RequireSyncReplica;
        transaction->ModifyRows(
            GetParameters()->TablePath.GetPath(),
            NameTable_,
            std::move(modifications),
            options);

        NConcurrency::WaitFor(transaction->Commit()).ThrowOnError();
        WriteErrorState_->ClearError();
        return true;
    } catch (const TErrorException& ex) {
        auto error = TError(ex);
        if (error.FindMatching(NYT::EErrorCode::Canceled)) {
            throw;
        }
        auto wrapped = TError("Failed to write to sorted dynamic table").With(error);
        WriteErrorState_->SetError(wrapped);
        YT_TLOG_WARNING("Retrying write to sorted dynamic table")
            .With(wrapped);
        return false;
    }
}

TFuture<void> TAsyncSink::DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 /*seqNo*/)
{
    // Serialize tablet writes: concurrent commits to the same sorted table
    // (especially chaos CRT) deadlock / hang on overlapping keys.
    const auto initialBackoff = GetDynamicParameters()->BackoffDuration;
    return NDetail::RunSerializedRetries(
        MakeWeak(this),
        WriteSemaphore_,
        GetContext()->SerializedInvoker,
        TExponentialBackoffOptions{
            .InvocationCount = std::numeric_limits<int>::max(),
            .MinBackoff = initialBackoff,
            .MaxBackoff = std::max(initialBackoff, TDuration::Minutes(1)),
        },
        [messages] (TAsyncSink* sink) {
            return sink->TryWriteBatch(messages);
        });
}

DEFINE_REFCOUNTED_TYPE(TAsyncSink);

////////////////////////////////////////////////////////////////////////////////

TSinkController::TSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TSinkControllerBase(std::move(context), std::move(dynamicContext))
    , Client_(GetParameters()->TablePath.GetCluster()
            ? GetContext()->ClientsCache->GetClient(*GetParameters()->TablePath.GetCluster())
            : GetContext()->GetClient())
    , UpdatePartitionCountErrorState_(GetContext()->StatusProfiler->ErrorState("/update_partition_count"))
{ }

void TSinkController::Init(IInitContextPtr initContext)
{
    initContext->WithPrefix("table_info")->InitClient<TInfoControllerState>(State_, "v0");
    Executor_ = New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TSinkController::TryUpdatePartitionCount, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(GetParameters()->UpdatePartitionCountPeriod));
    Executor_->Start();
    Executor_->ScheduleOutOfBand();
}

void TSinkController::Sync()
{ }

void TSinkController::Commit()
{ }

void TSinkController::TryUpdatePartitionCount()
{
    try {
        const auto& tablePath = GetParameters()->TablePath.GetPath();

        NApi::TGetNodeOptions options;
        options.Attributes = {"tablet_count", "type", "replicas"};
        auto ysonString = NConcurrency::WaitFor(Client_->GetNode(tablePath, options))
            .ValueOrThrow();
        auto node = NYTree::ConvertToNode(ysonString);
        const auto& attributes = node->Attributes();
        auto type = attributes.Get<NObjectClient::EObjectType>("type");

        int tabletCount = 0;
        if (type == NObjectClient::EObjectType::ChaosReplicatedTable) {
            auto location = FindEnabledReplica(
                attributes.Get<NYTree::IMapNodePtr>("replicas"),
                tablePath,
                "data");
            auto replicaClient = GetContext()->ClientsCache->GetClient(location.ClusterName);
            NApi::TGetNodeOptions replicaOptions;
            replicaOptions.Attributes = {"tablet_count"};
            replicaOptions.ReadFrom = NApi::EMasterChannelKind::Cache;
            auto replicaYson = NConcurrency::WaitFor(
                replicaClient->GetNode(location.Path, replicaOptions))
                .ValueOrThrow();
            tabletCount = NYTree::ConvertToNode(replicaYson)->Attributes().Get<int>("tablet_count");
        } else {
            tabletCount = attributes.Get<int>("tablet_count");
        }

        auto guard = Guard(StateLock_);
        State_->CachedPartitionCount = tabletCount;
        YT_TLOG_INFO("Table partition count was updated")
            .With("CurrentPartitionCount", State_->CachedPartitionCount);
        UpdatePartitionCountErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to update partition count")
            .With(ex)
            .With("table_path", GetParameters()->TablePath);
        UpdatePartitionCountErrorState_->SetError(error);
    }
}

std::optional<i64> TSinkController::GetReceiverChannelCount()
{
    auto guard = Guard(StateLock_);
    return State_->CachedPartitionCount;
}

DEFINE_REFCOUNTED_TYPE(TSinkController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable
