#include "sink.h"

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/queue_client/producer_client.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

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
    , Logger(TSyncSinkBase::Logger.WithTag("TablePath: %v", GetParameters()->TablePath))
    , NameTable_(GenerateNameTable(GetContext(), GetSpec(), GetParameters()))
{ }

void TSyncSink::DoInit()
{ }

void TSyncSink::DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages)
{
    YT_LOG_INFO("Synchronously modifying rows in table (MessagesCount: %v, AggregateColumns: %v, DeleteRows: %v)",
        std::ssize(messages),
        GetParameters()->AggregateColumns,
        GetParameters()->DeleteRows);
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
        NApi::TGetNodeOptions options;
        options.Attributes = {"tablet_count", "type"};
        auto ysonString = NConcurrency::WaitFor(Client_->GetNode(GetParameters()->TablePath.GetPath(), options))
            .ValueOrThrow();
        auto node = NYTree::ConvertToNode(ysonString);
        const auto& attributes = node->Attributes();
        auto type = attributes.Get<NObjectClient::EObjectType>("type");
        if (type == NObjectClient::EObjectType::ChaosReplicatedTable) {
            THROW_ERROR_EXCEPTION("No support for chaos yet")
                << TErrorAttribute("queue_path", GetParameters()->TablePath);
        }
        auto guard = Guard(StateLock_);
        State_->CachedPartitionCount = attributes.Get<int>("tablet_count");
        YT_LOG_INFO("Table partition count was updated (CurrentPartitionCount: %v)",
            State_->CachedPartitionCount);
        UpdatePartitionCountErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to update partition count") << ex;
        YT_LOG_ERROR(error);
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
