#include "sink.h"

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/queue_client/producer_client.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <util/generic/xrange.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TMessageContainer>
TSharedRange<NTableClient::TUnversionedRow> PackRows(
    const TMessageContainer& messages,
    const NTableClient::TNameTablePtr& nameTable,
    std::optional<int> flowMetaColumn)
{
    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;
    for (const auto& message : messages) {
        NTableClient::TUnversionedRowBuilder builder(message->PayloadSchema->GetColumnCount() + 1);
        for (int i = 0; i < message->Payload.Underlying().GetCount(); ++i) {
            const auto& column = message->PayloadSchema->Columns()[i];
            if (auto id = nameTable->FindId(column.Name())) {
                auto value = message->Payload.Underlying()[i];
                value.Id = *id;
                builder.AddValue(buffer->CaptureValue(value));
            }
        }
        if (flowMetaColumn) {
            const auto meta = BuildFlowQueueMeta(*message);
            const auto serializedMeta = NYson::ConvertToYsonString(meta, NYson::EYsonFormat::Binary);
            builder.AddValue(buffer->CaptureValue(NTableClient::MakeUnversionedAnyValue(serializedMeta.AsStringBuf(), *flowMetaColumn)));
        }
        // Values has been already captured.
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ false));
    }
    return MakeSharedRange(std::move(rows), std::move(buffer));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSyncQueueSink::TSyncQueueSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TSyncSinkBase::Logger.WithTag("QueuePath: %v", GetParameters()->QueuePath))
    , NameTable_(GenerateNameTable())
    , FlowMetaColumn_(
        GetParameters()->WriteFlowQueueMeta
            ? NameTable_->FindId(GetParameters()->FlowQueueMetaColumn)
            : std::nullopt)
{ }

void TSyncQueueSink::DoInit()
{ }

void TSyncQueueSink::DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages)
{
    YT_TLOG_INFO("Synchronously writing messages to queue")
        .With("MessagesCount", std::ssize(messages));
    auto range = PackRows(messages, NameTable_, FlowMetaColumn_);
    transaction->WriteRows(GetParameters()->QueuePath.GetPath(), NameTable_, std::move(range));
}

NTableClient::TNameTablePtr TSyncQueueSink::GenerateNameTable() const
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected exactly one input stream id, but got %v",
            GetSpec()->InputStreamIds.size());
    }
    auto streamId = *GetSpec()->InputStreamIds.begin();
    auto schema = GetContext()->StreamSpecStorage->GetSchema(streamId);

    NTableClient::TNameTablePtr nameTable;
    if (GetParameters()->ColumnFilter) {
        nameTable = New<NTableClient::TNameTable>();
        for (const auto& field : *GetParameters()->ColumnFilter) {
            if (schema->FindColumn(field) == nullptr) {
                THROW_ERROR_EXCEPTION("Column %v not found in schema",
                    field);
            }
            nameTable->RegisterNameOrThrow(field);
        }
    } else {
        nameTable = NTableClient::TNameTable::FromSchema(*schema);
    }
    if (GetParameters()->WriteFlowQueueMeta) {
        nameTable->RegisterNameOrThrow(GetParameters()->FlowQueueMetaColumn);
    }
    return nameTable;
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueWriterBase::TRequestLimiter::TRequestLimiter(i64 maxCount, i64 maxSize)
    : MaxCount_(maxCount)
    , MaxSize_(maxSize)
{ }

void TAsyncQueueWriterBase::TRequestLimiter::Add(const TRequest& request)
{
    Count_ += 1;
    Size_ += std::get<NTableClient::TUnversionedOwningRow>(request).GetSpaceUsed();
}

bool TAsyncQueueWriterBase::TRequestLimiter::IsFull() const
{
    return Count_ >= MaxCount_ || Size_ >= MaxSize_;
}

TAsyncQueueWriterBase::TAsyncQueueWriterBase(
    TSinkContextPtr context,
    TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
    NTableClient::TNameTablePtr nameTable,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : Context_(std::move(context))
    , DynamicParameters_(std::move(dynamicParameters))
    , Logger(std::move(logger))
    , NameTable_(std::move(nameTable))
    , ErrorState_(std::move(errorState))
    , Requests_(New<NConcurrency::TNonblockingBatcher<TRequest, TRequestLimiter>>(
        TRequestLimiter(DynamicParameters_.Acquire()->MaxRowsPerWrite, DynamicParameters_.Acquire()->MaxBytesPerWrite),
        DynamicParameters_.Acquire()->WritePeriod,
        true))
{ }

void TAsyncQueueWriterBase::InitSession(const std::string& producerId)
{
    YT_TLOG_INFO("Initializing async queue writer")
        .With("ProducerId", producerId);
    Executor_ = BIND(&TAsyncQueueWriterBase::Execute, MakeWeak(this), producerId)
        .AsyncVia(Context_->SerializedInvoker)
        .Run();
}

TFuture<void> TAsyncQueueWriterBase::Write(const NTableClient::TUnversionedOwningRow& row)
{
    auto promise = NewPromise<void>();
    auto request = TRequest{row, promise};
    Requests_->Enqueue(std::move(request));
    return promise.ToFuture();
}

void TAsyncQueueWriterBase::Reconfigure(TDynamicAsyncQueueWriterParametersPtr dynamicParameters)
{
    DynamicParameters_ = dynamicParameters;
    Requests_->UpdateSettings(
        dynamicParameters->WritePeriod,
        TRequestLimiter(dynamicParameters->MaxRowsPerWrite, dynamicParameters->MaxBytesPerWrite),
        true);
}

void TAsyncQueueWriterBase::Execute(TWeakPtr<TAsyncQueueWriterBase> weakThis, std::string producerId)
{
    while (true) {
        TFuture<std::vector<TRequest>> requestsFuture;
        if (auto strongThis = weakThis.Lock()) {
            requestsFuture = strongThis->Requests_->DequeueBatch();
        } else {
            break;
        }

        auto requests = NConcurrency::WaitFor(requestsFuture).ValueOrThrow();
        if (requests.empty()) {
            continue;
        }

        while (auto strongThis = weakThis.Lock()) {
            if (strongThis->TryExecuteIteration(producerId, requests)) {
                break;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueWriter::TAsyncQueueWriter(
    TSinkContextPtr context,
    TAsyncQueueWriterParametersPtr parameters,
    TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
    NTableClient::TNameTablePtr nameTable,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : TAsyncQueueWriterBase(
        std::move(context),
        std::move(dynamicParameters),
        std::move(nameTable),
        std::move(errorState),
        logger.WithTag("QueuePath: %v, ProducerPath: %v",
            parameters->QueuePath,
            parameters->ProducerPath))
    , Parameters_(std::move(parameters))
    , Client_(Context_->ClientsCache->GetClient(*Parameters_->QueuePath.GetCluster()))
    , ProducerClient_(NQueueClient::CreateProducerClient(Client_, Parameters_->ProducerPath))
    , Session_(nullptr)
{ }

bool TAsyncQueueWriter::TryExecuteIteration(const std::string& producerId, const std::vector<TRequest>& requests)
{
    try {
        auto buffer = New<NTableClient::TRowBuffer>();
        std::vector<NTableClient::TUnversionedRow> rows;
        for (auto& request : requests) {
            rows.push_back(buffer->CaptureRow(std::get<NTableClient::TUnversionedOwningRow>(request)));
        }
        auto session = GetSession(producerId);
        Y_UNUSED(session->Write(MakeSharedRange(std::move(rows), std::move(buffer))));
        NConcurrency::WaitFor(session->Flush()).ThrowOnError();
        for (auto& request : requests) {
            std::get<TPromise<void>>(request).TrySet();
        }
        YT_TLOG_INFO("Asynchronously wrote messages to the queue")
            .With("Count", std::ssize(requests));
        ErrorState_->ClearError();
        return true;
    } catch (const std::exception& ex) {
        ErrorState_->SetError(TError("Failed to write to the queue") << ex);
        ResetSession();
        NConcurrency::TDelayedExecutor::WaitForDuration(DynamicParameters_.Acquire()->BackoffDuration);
        return false;
    }
}

void TAsyncQueueWriter::ResetSession()
{
    Session_ = {};
}

NQueueClient::IProducerSessionPtr TAsyncQueueWriter::GetSession(const std::string& producerId)
{
    if (Session_) {
        return Session_;
    }

    auto sessionId = NQueueClient::TQueueProducerSessionId(TString(producerId));
    NQueueClient::TProducerSessionOptions options;
    options.BatchOptions.RowCount = std::numeric_limits<i64>::max();
    options.BatchOptions.ByteSize = std::numeric_limits<i64>::max();
    options.RequireSyncReplica = Parameters_->RequireSyncReplica;
    Session_ = NConcurrency::WaitFor(ProducerClient_->CreateSession(
        Parameters_->QueuePath,
        NameTable_,
        sessionId,
        options))
        .ValueOrThrow();
    return Session_;
}

////////////////////////////////////////////////////////////////////////////////

TAsyncMultiClusterQueueWriter::TAsyncMultiClusterQueueWriter(
    TSinkContextPtr context,
    TAsyncMultiClusterQueueWriterParametersPtr parameters,
    TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
    NTableClient::TNameTablePtr nameTable,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : TAsyncQueueWriterBase(
        std::move(context),
        std::move(dynamicParameters),
        std::move(nameTable),
        std::move(errorState),
        logger.WithTag("QueuePath: %v, ProducerPath: %v",
            parameters->QueuePath,
            parameters->ProducerPath))
    , Parameters_(std::move(parameters))
    , DataByCluster_()
    , CurrentClusterIndex_(0)
{
    if (auto cluster = Parameters_->QueuePath.GetCluster()) {
        ConstructByCluster(*cluster);
    } else if (auto clusters = Parameters_->QueuePath.GetClusters()) {
        for (const auto& cluster : *clusters) {
            ConstructByCluster(cluster);
        }
    }
}

void TAsyncMultiClusterQueueWriter::ConstructByCluster(const std::string& cluster)
{
    auto client = Context_->ClientsCache->GetClient(cluster);
    DataByCluster_.emplace(cluster,
        TClusterData{
            .Client = client,
            .ProducerClient = NQueueClient::CreateProducerClient(client, Parameters_->ProducerPath),
            .Session = nullptr,
        });
}

bool TAsyncMultiClusterQueueWriter::TryExecuteIteration(const std::string& producerId, const std::vector<TRequest>& requests)
{
    auto error = TError("Failed to write to the queue on all clusters!");
    for (auto _ : xrange(DataByCluster_.size())) {
        auto it = DataByCluster_.begin();
        std::advance(it, CurrentClusterIndex_);
        if (auto internalError = TryExecuteIterationOnCluster(producerId, requests, it); !internalError.IsOK()) {
            CurrentClusterIndex_ = (CurrentClusterIndex_ + 1) % DataByCluster_.size();
            error <<= internalError;
        } else {
            ErrorState_->ClearError();
            return true;
        }
    }
    ErrorState_->SetError(error);
    NConcurrency::TDelayedExecutor::WaitForDuration(DynamicParameters_.Acquire()->BackoffDuration);
    return false;
}

TError TAsyncMultiClusterQueueWriter::TryExecuteIterationOnCluster(
    const std::string& producerId,
    const std::vector<TRequest>& requests,
    TClusterDataMap::iterator& it)
{
    auto& clusterData = it->second;
    try {
        auto buffer = New<NTableClient::TRowBuffer>();
        std::vector<NTableClient::TUnversionedRow> rows;
        for (auto& request : requests) {
            rows.push_back(buffer->CaptureRow(std::get<NTableClient::TUnversionedOwningRow>(request)));
        }
        auto session = GetSession(producerId, clusterData);
        Y_UNUSED(session->Write(MakeSharedRange(std::move(rows), std::move(buffer))));
        NConcurrency::WaitFor(session->Flush()).ThrowOnError();
        for (auto& request : requests) {
            std::get<TPromise<void>>(request).TrySet();
        }
        YT_TLOG_INFO("Asynchronously wrote messages to the queue")
            .With("Count", std::ssize(requests));
        return TError();
    } catch (const std::exception& ex) {
        YT_TLOG_WARNING("Failed to write to the queue on cluster, reinit session")
            .With("Cluster", it->first)
            .With(ex);
        clusterData.Session = {};
        return TError(ex);
    }
}

NQueueClient::IProducerSessionPtr TAsyncMultiClusterQueueWriter::GetSession(const std::string& producerId, TClusterData& clusterData)
{
    auto& session = clusterData.Session;
    if (session) {
        return session;
    }

    auto sessionId = NQueueClient::TQueueProducerSessionId(TString(producerId));
    NQueueClient::TProducerSessionOptions options;
    options.BatchOptions.RowCount = std::numeric_limits<i64>::max();
    options.BatchOptions.ByteSize = std::numeric_limits<i64>::max();
    options.RequireSyncReplica = Parameters_->RequireSyncReplica;

    session = NConcurrency::WaitFor(clusterData.ProducerClient->CreateSession(
        Parameters_->QueuePath,
        NameTable_,
        sessionId,
        options))
        .ValueOrThrow();
    return session;
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueSinkImpl::TAsyncQueueSinkImpl(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TDelegatingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , NameTable_(GenerateNameTable())
    , SeqNoColumn_(NameTable_->GetIdOrThrow(NTableClient::SequenceNumberColumnName))
    , FlowMetaColumn_(
        GetParameters()->WriteFlowQueueMeta
            ? NameTable_->FindId(GetParameters()->FlowQueueMetaColumn)
            : std::nullopt)
{ }

TAsyncQueueSinkImpl::~TAsyncQueueSinkImpl()
{
    std::vector<TIntrusivePtr<TRefCounted>> prevent;
    if (Writer_) {
        prevent.push_back(Writer_);
    }
    SuspendDestructionGuarded(std::move(prevent));
}

void TAsyncQueueSinkImpl::DoInit(const std::string& producerId)
{
    Writer_ = CreateWriter();
    Writer_->InitSession(producerId);
}

NTableClient::TUnversionedOwningRow TAsyncQueueSinkImpl::BuildRow(const TOutputMessageConstPtr& message, i64 seqNo) const
{
    NTableClient::TUnversionedOwningRowBuilder builder;
    for (int i = 0; i < message->Payload.Underlying().GetCount(); ++i) {
        const auto& column = message->PayloadSchema->Columns()[i];
        if (auto id = NameTable_->FindId(column.Name())) {
            auto value = message->Payload.Underlying()[i];
            value.Id = *id;
            builder.AddValue(value);
        }
    }
    builder.AddValue(NTableClient::MakeUnversionedInt64Value(seqNo, SeqNoColumn_));
    if (FlowMetaColumn_) {
        const auto meta = BuildFlowQueueMeta(*message);
        const auto serializedMeta = NYson::ConvertToYsonString(meta, NYson::EYsonFormat::Binary);
        builder.AddValue(NTableClient::MakeUnversionedAnyValue(serializedMeta.AsStringBuf(), *FlowMetaColumn_));
    }
    return builder.FinishRow();
}

std::pair<TFuture<void>, ui64> TAsyncQueueSinkImpl::DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo)
{
    auto row = BuildRow(message, seqNo);
    return {Writer_->Write(row), message->ByteSize};
}

NTableClient::TNameTablePtr TAsyncQueueSinkImpl::GenerateNameTable() const
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected exactly one input stream id, but got %v",
            GetSpec()->InputStreamIds.size());
    }
    auto streamId = *GetSpec()->InputStreamIds.begin();
    auto schema = GetContext()->StreamSpecStorage->GetSchema(streamId);

    NTableClient::TNameTablePtr nameTable;
    if (GetParameters()->ColumnFilter) {
        nameTable = New<NTableClient::TNameTable>();
        for (const auto& field : *GetParameters()->ColumnFilter) {
            if (schema->FindColumn(field) == nullptr) {
                THROW_ERROR_EXCEPTION("Column %v not found in schema",
                    field);
            }
            nameTable->RegisterNameOrThrow(field);
        }
    } else {
        nameTable = NTableClient::TNameTable::FromSchema(*schema);
    }
    nameTable->RegisterNameOrThrow(NTableClient::SequenceNumberColumnName);
    if (GetParameters()->WriteFlowQueueMeta) {
        nameTable->RegisterNameOrThrow(GetParameters()->FlowQueueMetaColumn);
    }
    return nameTable;
}

IAsyncQueueWriterPtr TAsyncQueueSink::CreateWriter()
{
    return New<TAsyncQueueWriter>(
        GetContext(),
        GetParameters(),
        GetDynamicParameters(),
        NameTable_,
        GetContext()->StatusProfiler->ErrorState("/async_queue_writer"),
        TDelegatingAsyncSinkBase::Logger);
}

IAsyncQueueWriterPtr TAsyncMultiClusterQueueSink::CreateWriter()
{
    return New<TAsyncMultiClusterQueueWriter>(
        GetContext(),
        GetParameters(),
        GetDynamicParameters(),
        NameTable_,
        GetContext()->StatusProfiler->ErrorState("/async_multi_cluster_queue_writer"),
        TDelegatingAsyncSinkBase::Logger);
}

////////////////////////////////////////////////////////////////////////////////

TQueueSinkController::TQueueSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TSinkControllerBase(std::move(context), std::move(dynamicContext))
    , Client_(GetParameters()->QueuePath.GetCluster()
            ? GetContext()->ClientsCache->GetClient(*GetParameters()->QueuePath.GetCluster())
            : GetContext()->GetClient())
    , Info_(New<TQueueInfoController>(
        GetParameters(),
        Client_,
        GetContext()->Invoker,
        GetContext()->Logger.WithTag("QueuePath: %v", GetParameters()->QueuePath),
        GetContext()->StatusProfiler->WithPrefix("/queue_info")))
    , HeartbeatExecutor_(New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TQueueSinkController::WriteFlowQueueMeta, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(GetDynamicParameters()->FlowQueueMetaHeartbeatPeriod)))
    , WriteMetaErrorState_(GetContext()->StatusProfiler->ErrorState("write_meta"))
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkControllerContextPtr& /*dynamicContext*/) {
        HeartbeatExecutor_->SetPeriod(this->GetDynamicParameters()->FlowQueueMetaHeartbeatPeriod);
    }));
}

void TQueueSinkController::Init(IInitContextPtr initContext)
{
    Info_->Init(initContext->WithPrefix("queue_info"));
    if (GetParameters()->WriteFlowQueueMeta) {
        HeartbeatExecutor_->Start();
    }
}

void TQueueSinkController::Sync()
{
    Info_->Sync();
}

void TQueueSinkController::Commit()
{
    Info_->Commit();
}

std::optional<i64> TQueueSinkController::GetReceiverChannelCount()
{
    return Info_->GetPartitionCount();
}

void TQueueSinkController::WriteFlowQueueMeta()
{
    try {
        auto watermarkState = GetWatermarkState();
        if (!watermarkState) {
            YT_TLOG_WARNING("Watermark state is not available, skipping heartbeat");
            return;
        }

        auto partitions = Info_->GetPartitionCount();
        if (!partitions) {
            YT_TLOG_WARNING("Partition count is not available, skipping heartbeat");
            return;
        }

        auto transaction = NConcurrency::WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet, NApi::TTransactionStartOptions())).ValueOrThrow();

        TSystemTimestamp watermark = InfinitySystemTimestamp;
        for (const auto& streamId : GetSpec()->InputStreamIds) {
            watermark = std::min(watermark, watermarkState->GetEventWatermark(streamId));
        }
        TFlowQueueMeta meta;
        meta.EventWatermark = watermark;
        meta.PureHeartbeat = true;
        auto serializedMeta = NYson::ConvertToYsonString(meta, NYson::EYsonFormat::Binary);
        YT_TLOG_INFO("Writing flow queue meta heartbeat")
            .With("EventWatermark", meta.EventWatermark);

        auto nameTable = New<NTableClient::TNameTable>();
        auto metaField = nameTable->RegisterNameOrThrow(GetParameters()->FlowQueueMetaColumn);
        auto tabletField = nameTable->RegisterNameOrThrow("$tablet_index");
        auto buffer = New<NTableClient::TRowBuffer>();
        std::vector<NTableClient::TUnversionedRow> rows;
        for (int i = 0; i < *partitions; ++i) {
            NTableClient::TUnversionedRowBuilder builder(2);
            builder.AddValue(NTableClient::MakeUnversionedAnyValue(serializedMeta.AsStringBuf(), metaField));
            builder.AddValue(NTableClient::MakeUnversionedInt64Value(i, tabletField));
            rows.push_back(buffer->CaptureRow(builder.GetRow()));
        }
        auto range = MakeSharedRange(std::move(rows), std::move(buffer));
        transaction->WriteRows(GetParameters()->QueuePath.GetPath(), nameTable, std::move(range));
        NConcurrency::WaitFor(transaction->Commit()).ValueOrThrow();
        WriteMetaErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to write flow queue meta heartbeat") << ex;
        // TODO: pass to public controller log.
        WriteMetaErrorState_->SetError(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

TMultiClusterQueueSinkController::TMultiClusterQueueSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TSinkControllerBase(std::move(context), std::move(dynamicContext))
    , ClientByCluster_()
    , InfoByCluster_()
    , HeartbeatExecutor_(New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TMultiClusterQueueSinkController::WriteFlowQueueMeta, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(GetDynamicParameters()->FlowQueueMetaHeartbeatPeriod)))
    , WriteMetaErrorState_(GetContext()->StatusProfiler->ErrorState("/write_meta"))
{
    if (auto cluster = GetParameters()->QueuePath.GetCluster()) {
        ConstructByCluster(
            *cluster,
            GetContext()->ClientsCache->GetClient(*cluster));
    } else if (auto clusters = GetParameters()->QueuePath.GetClusters()) {
        for (const auto& cluster : *clusters) {
            ConstructByCluster(
                cluster,
                GetContext()->ClientsCache->GetClient(cluster));
        }
    } else {
        auto pipelineCluster = *GetContext()->PipelinePath.GetCluster();
        ConstructByCluster(pipelineCluster, GetContext()->ClientsCache->GetClient(pipelineCluster));
    }
    SubscribeReconfigured(BIND([this] (const TDynamicSinkControllerContextPtr& /*dynamicContext*/) {
        HeartbeatExecutor_->SetPeriod(this->GetDynamicParameters()->FlowQueueMetaHeartbeatPeriod);
    }));
}

void TMultiClusterQueueSinkController::ConstructByCluster(const std::string& cluster, const NApi::IClientPtr client)
{
    ClientByCluster_[cluster] = client;
    auto info = InfoByCluster_[cluster] = New<TQueueInfoController>(
        GetParameters(),
        client,
        GetContext()->Invoker,
        GetContext()->Logger.WithTag("QueuePath: %v", GetParameters()->QueuePath).WithTag("Cluster: %v", cluster),
        GetContext()->StatusProfiler->WithPrefix(Format("/queue_info/clusters/%v", cluster)));
}

void TMultiClusterQueueSinkController::Init(IInitContextPtr initContext)
{
    for (const auto& [cluster, info] : InfoByCluster_) {
        info->Init(initContext->WithPrefix("queue_info_" + cluster));
    }
}

void TMultiClusterQueueSinkController::Sync()
{
    for (const auto& [_, info] : InfoByCluster_) {
        info->Sync();
    }
}

void TMultiClusterQueueSinkController::Commit()
{
    for (const auto& [_, info] : InfoByCluster_) {
        info->Commit();
    }
}

std::optional<i64> TMultiClusterQueueSinkController::GetReceiverChannelCount()
{
    std::optional<int> maxParititonCount;
    for (const auto& [_, info] : InfoByCluster_) {
        if (info->GetPartitionCount().has_value()) {
            if (!maxParititonCount.has_value()) {
                maxParititonCount = info->GetPartitionCount();
            } else {
                maxParititonCount = std::max(*maxParititonCount, *info->GetPartitionCount());
            }
        }
    }
    return maxParititonCount;
}

void TMultiClusterQueueSinkController::WriteFlowQueueMeta()
{
    try {
        auto watermarkState = GetWatermarkState();
        if (!watermarkState) {
            YT_TLOG_WARNING("Watermark state is not available, skipping heartbeat");
            return;
        }

        TSystemTimestamp watermark = InfinitySystemTimestamp;
        for (const auto& streamId : GetSpec()->InputStreamIds) {
            watermark = std::min(watermark, watermarkState->GetEventWatermark(streamId));
        }
        TFlowQueueMeta meta;
        meta.EventWatermark = watermark;
        meta.PureHeartbeat = true;
        auto serializedMeta = NYson::ConvertToYsonString(meta, NYson::EYsonFormat::Binary);
        YT_TLOG_INFO("Writing flow queue meta heartbeat")
            .With("EventWatermark", meta.EventWatermark);

        auto nameTable = New<NTableClient::TNameTable>();
        auto metaField = nameTable->RegisterNameOrThrow(GetParameters()->FlowQueueMetaColumn);
        auto tabletField = nameTable->RegisterNameOrThrow("$tablet_index");

        for (const auto& [cluster, info] : InfoByCluster_) {
            auto partitions = info->GetPartitionCount();
            if (!partitions) {
                YT_TLOG_WARNING("Partition count is not available for cluster, skipping heartbeat on this cluster")
                    .With("Cluster", cluster);
                continue;
            }

            auto iter = ClientByCluster_.find(cluster);
            YT_VERIFY(iter != ClientByCluster_.end());
            auto transaction = NConcurrency::WaitFor(iter->second->StartTransaction(NTransactionClient::ETransactionType::Tablet, NApi::TTransactionStartOptions())).ValueOrThrow();

            auto buffer = New<NTableClient::TRowBuffer>();
            std::vector<NTableClient::TUnversionedRow> rows;
            for (int i = 0; i < *partitions; ++i) {
                NTableClient::TUnversionedRowBuilder builder(2);
                builder.AddValue(NTableClient::MakeUnversionedAnyValue(serializedMeta.AsStringBuf(), metaField));
                builder.AddValue(NTableClient::MakeUnversionedInt64Value(i, tabletField));
                rows.push_back(buffer->CaptureRow(builder.GetRow()));
            }
            auto range = MakeSharedRange(std::move(rows), std::move(buffer));
            transaction->WriteRows(GetParameters()->QueuePath.GetPath(), nameTable, std::move(range));
            NConcurrency::WaitFor(transaction->Commit()).ValueOrThrow();
        }
        WriteMetaErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to write flow queue meta heartbeat") << ex;
        // TODO: pass to public controller log.
        WriteMetaErrorState_->SetError(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
