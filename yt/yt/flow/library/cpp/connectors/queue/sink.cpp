#include "sink.h"

#include <yt/yt/flow/library/cpp/resources/yt_client_factory.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/connectors/common/flow_queue_meta.h>
#include <yt/yt/flow/library/cpp/connectors/queue/tablet_router.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TQueueProducerUserMeta
    : public NYTree::TYsonStruct
{
    std::string LastMessageId;

    REGISTER_YSON_STRUCT(TQueueProducerUserMeta);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("last_message_id", &TThis::LastMessageId);
    }
};

DEFINE_REFCOUNTED_TYPE(TQueueProducerUserMeta);

TMessageId GetLastMessageIdFromUserMeta(const NYTree::INodePtr& userMeta)
{
    if (!userMeta) {
        return {};
    }
    return TMessageId(NYTree::ConvertTo<TIntrusivePtr<TQueueProducerUserMeta>>(userMeta)->LastMessageId);
}

void WaitBackoff(std::optional<TBackoffStrategy>* backoffStrategy, TDuration minBackoff)
{
    auto backoffOptions = TExponentialBackoffOptions{
        .InvocationCount = std::numeric_limits<int>::max(),
        .MinBackoff = minBackoff,
        .MaxBackoff = std::max(minBackoff, TDuration::Minutes(1)),
    };
    auto& strategy = *backoffStrategy;
    if (!strategy) {
        strategy.emplace(backoffOptions);
    } else if (strategy->GetOptions() != backoffOptions) {
        strategy->UpdateOptions(backoffOptions);
        strategy->Restart();
    }
    strategy->Next();
    NConcurrency::TDelayedExecutor::WaitForDuration(strategy->GetBackoff());
}

// Writes payload rows directly (base queue layout). With |router| set, the tablet index is
// computed per message and written to |tabletIndexColumn| (single pass, no grouping).
void AppendPackedRows(
    std::vector<NTableClient::TUnversionedRow>& rows,
    const NTableClient::TRowBufferPtr& buffer,
    const std::deque<TOutputMessageConstPtr>& messages,
    const NTableClient::TNameTablePtr& nameTable,
    std::optional<int> flowMetaColumn,
    std::optional<int> tabletIndexColumn,
    TTabletRouter* router)
{
    for (const auto& message : messages) {
        NTableClient::TUnversionedRowBuilder builder(message->PayloadSchema->GetColumnCount() + 2);
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
        if (tabletIndexColumn) {
            builder.AddValue(NTableClient::MakeUnversionedInt64Value(router->GetTabletIndex(message->Payload), *tabletIndexColumn));
        }
        // Values has been already captured.
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ false));
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSyncQueueSink::TSyncQueueSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , Logger(TSyncSinkBase::Logger.WithTag("QueuePath", GetParameters()->QueuePath))
    , NameTable_(GenerateNameTable())
    , FlowMetaColumn_(
        GetParameters()->WriteFlowQueueMeta
            ? NameTable_->FindId(GetParameters()->FlowQueueMetaColumn)
            : std::nullopt)
    , TabletIndexColumn_(
        IsTabletRoutingEnabled(*GetParameters())
            ? std::optional<int>(NameTable_->GetIdOrThrow(NTableClient::TabletIndexColumnName))
            : std::nullopt)
    , TabletRouter_(CreateTabletRouter())
{ }

void TSyncQueueSink::DoInit()
{ }

void TSyncQueueSink::DoDistribute(NApi::IDynamicTableTransactionPtr transaction, const std::deque<TOutputMessageConstPtr>& messages)
{
    if (messages.empty()) {
        return;
    }
    if (TabletRouter_) {
        TabletRouter_->Start();
    }
    YT_TLOG_INFO("Synchronously writing messages to queue")
        .With("MessagesCount", std::ssize(messages));

    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;
    AppendPackedRows(rows, buffer, messages, NameTable_, FlowMetaColumn_, TabletIndexColumn_, TabletRouter_.Get());
    transaction->WriteRows(
        GetParameters()->QueuePath.GetPath(),
        NameTable_,
        MakeSharedRange(std::move(rows), std::move(buffer)));
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
    if (IsTabletRoutingEnabled(*GetParameters())) {
        nameTable->RegisterNameOrThrow(NTableClient::TabletIndexColumnName);
    }
    return nameTable;
}

TTabletRouterPtr TSyncQueueSink::CreateTabletRouter() const
{
    auto streamId = *GetSpec()->InputStreamIds.begin();
    auto schema = GetContext()->StreamSpecStorage->GetSchema(streamId);
    return NYT::NFlow::CreateTabletRouter(
        *GetParameters(),
        GetParameters()->QueuePath,
        GetParameters()->UpdatePartitionCountPeriod,
        schema,
        GetContext(),
        Logger);
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueWriter::TRequestLimiter::TRequestLimiter(i64 maxCount, i64 maxSize)
    : MaxCount_(maxCount)
    , MaxSize_(maxSize)
{ }

void TAsyncQueueWriter::TRequestLimiter::Add(const TRequest& request)
{
    Count_ += std::ssize(request.Rows);

    for (const auto& row : request.Rows) {
        Size_ += row.GetSpaceUsed();
    }
}

bool TAsyncQueueWriter::TRequestLimiter::IsFull() const
{
    return Count_ >= MaxCount_ || Size_ >= MaxSize_;
}

TAsyncQueueWriter::TAsyncQueueWriter(
    TSinkContextPtr context,
    TAsyncQueueWriterParametersBasePtr parameters,
    std::vector<std::string> clusters,
    TDynamicAsyncQueueWriterParametersPtr dynamicParameters,
    NTableClient::TNameTablePtr nameTable,
    IStatusErrorStatePtr errorState,
    NLogging::TLogger logger)
    : Logger(std::move(logger)
            .WithTag("QueuePath", parameters->QueuePath)
            .WithTag("ProducerPath", parameters->ProducerPath))
    , Context_(std::move(context))
    , Parameters_(std::move(parameters))
    , NameTable_(std::move(nameTable))
    , ErrorState_(std::move(errorState))
    , DynamicParameters_(std::move(dynamicParameters))
    , Requests_(New<NConcurrency::TNonblockingBatcher<TRequest, TRequestLimiter>>(
        TRequestLimiter(DynamicParameters_.Acquire()->MaxRowsPerWrite, DynamicParameters_.Acquire()->MaxBytesPerWrite),
        DynamicParameters_.Acquire()->WritePeriod,
        true))
{
    YT_VERIFY(!clusters.empty());
    Clusters_.reserve(clusters.size());
    for (const auto& cluster : clusters) {
        Clusters_.push_back(TClusterData{
            .Name = cluster,
            .Client = Context_->ClientsCache->GetClient(cluster),
        });
    }
}

void TAsyncQueueWriter::Init(IInitContextPtr initContext, bool atMostOnce)
{
    AtMostOnce_ = atMostOnce;
    if (atMostOnce) {
        // Fresh IDs create extra producer rows, but a persistent ID would fence
        // the previous writer during its termination grace period.
        InitSession(ToString(TGuid::Create()));
        return;
    }

    initContext->InitClient(State_, "v1");
    auto& producerId = State_->ProducerId;
    if (producerId.empty()) {
        producerId = ToString(TGuid::Create());
    }
    InitSession(producerId);
}

void TAsyncQueueWriter::InitSession(const std::string& producerId)
{
    YT_TLOG_INFO("Initializing async queue writer")
        .With("ProducerId", producerId);
    StartSession(&Clusters_.front(), producerId);
    Executor_ = BIND(&TAsyncQueueWriter::Execute, MakeWeak(this), producerId)
        .AsyncVia(Context_->SerializedInvoker)
        .Run();
}

void TAsyncQueueWriter::Terminate()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    auto error = TError(NYT::EErrorCode::Canceled, "Async queue writer terminated");
    if (Executor_) {
        Executor_.Cancel(error);
    }
    for (const auto& cluster : Clusters_) {
        if (cluster.SessionFuture) {
            cluster.SessionFuture.Cancel(error);
        }
    }
    Requests_->Drain();
}

void TAsyncQueueWriter::Write(
    NTableClient::TUnversionedOwningRow row,
    TMessageId lastMessageId,
    TClosure onWritten)
{
    TRows rows;
    rows.push_back(std::move(row));
    WriteRows(std::move(rows), std::move(lastMessageId), std::move(onWritten));
}

void TAsyncQueueWriter::WriteRows(
    TRows rows,
    TMessageId lastMessageId,
    TClosure onWritten)
{
    YT_VERIFY(!rows.empty());

    Requests_->Enqueue(TRequest{
        .Rows = std::move(rows),
        .OnWritten = std::move(onWritten),
        .LastMessageId = std::move(lastMessageId),
    });
}

void TAsyncQueueWriter::Reconfigure(TDynamicAsyncQueueWriterParametersPtr dynamicParameters)
{
    DynamicParameters_ = dynamicParameters;
    Requests_->UpdateSettings(
        dynamicParameters->WritePeriod,
        TRequestLimiter(dynamicParameters->MaxRowsPerWrite, dynamicParameters->MaxBytesPerWrite),
        true);
}

void TAsyncQueueWriter::ThrowIfFatalError() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (FatalError_) {
        FatalError_->ThrowOnError();
    }
}

void TAsyncQueueWriter::Execute(TWeakPtr<TAsyncQueueWriter> weakThis, std::string producerId)
{
    while (true) {
        TFuture<std::vector<TRequest>> requestsFuture;
        if (auto strongThis = weakThis.Lock()) {
            requestsFuture = strongThis->Requests_->DequeueBatch();
        } else {
            break;
        }

        auto requestsResult = NConcurrency::WaitFor(requestsFuture);
        if (!requestsResult.IsOK()) {
            break;
        }
        auto requests = std::move(requestsResult).Value();
        requestsFuture = {};
        if (requests.empty()) {
            continue;
        }

        std::optional<TBackoffStrategy> backoffStrategy;
        while (auto strongThis = weakThis.Lock()) {
            if (strongThis->TryExecuteIteration(producerId, &requests)) {
                break;
            }

            auto minBackoff = strongThis->DynamicParameters_.Acquire()->BackoffDuration;
            strongThis.Reset();
            if (weakThis.IsExpired()) {
                break;
            }

            WaitBackoff(&backoffStrategy, minBackoff);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TMessageId> TAsyncQueueWriter::GetInitialMessageId() const
{
    YT_VERIFY(std::ssize(Clusters_) == 1);
    return Clusters_.front().SessionFuture.Apply(BIND([] (const TSessionResult& result) {
        return GetLastMessageIdFromUserMeta(result.UserMeta);
    }));
}

bool TAsyncQueueWriter::HasSingleCluster() const
{
    return std::ssize(Clusters_) == 1;
}

bool TAsyncQueueWriter::TryExecuteIteration(const std::string& producerId, std::vector<TRequest>* requests)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Context_->SerializedInvoker);
    if (FatalError_) {
        requests->clear();
        return true;
    }

    auto error = TError("Failed to write to the queue");
    bool written = false;
    for (ssize_t attempt = 0; attempt < std::ssize(Clusters_); ++attempt) {
        auto& cluster = Clusters_[CurrentClusterIndex_];
        bool commitStarted = false;
        try {
            const auto& session = GetSession(&cluster, producerId);
            ssize_t firstRequestIndex = 0;
            while (firstRequestIndex < std::ssize(*requests) && (*requests)[firstRequestIndex].LastMessageId <= cluster.LastMessageId) {
                ++firstRequestIndex;
            }

            if (firstRequestIndex < std::ssize(*requests)) {
                auto buffer = New<NTableClient::TRowBuffer>();
                std::vector<NTableClient::TUnversionedRow> rows;
                rows.reserve(std::ssize(*requests) - firstRequestIndex);
                for (ssize_t requestIndex = firstRequestIndex; requestIndex < std::ssize(*requests); ++requestIndex) {
                    for (const auto& row : (*requests)[requestIndex].Rows) {
                        rows.push_back(buffer->CaptureRow(row));
                    }
                }
                auto transaction = NConcurrency::WaitFor(cluster.Client->StartTransaction(NTransactionClient::ETransactionType::Tablet))
                    .ValueOrThrow();
                NApi::TPushQueueProducerOptions options;
                options.RequireSyncReplica = Parameters_->RequireSyncReplica;
                options.SequenceNumber = NQueueClient::TQueueProducerSequenceNumber(cluster.PhysicalSequenceNumber.Underlying() + 1);
                auto userMeta = New<TQueueProducerUserMeta>();
                userMeta->LastMessageId = std::string(requests->back().LastMessageId.Underlying());
                options.UserMeta = NYTree::ConvertToNode(userMeta);
                auto pushResult = NConcurrency::WaitFor(transaction->PushQueueProducer(
                    Parameters_->ProducerPath,
                    Parameters_->QueuePath,
                    NQueueClient::TQueueProducerSessionId(producerId),
                    session.Epoch,
                    NameTable_,
                    MakeSharedRange(std::move(rows), std::move(buffer)),
                    options))
                    .ValueOrThrow();
                commitStarted = true;
                NConcurrency::WaitFor(transaction->Commit())
                    .ThrowOnError();
                cluster.PhysicalSequenceNumber = pushResult.LastSequenceNumber;
                cluster.LastMessageId = requests->back().LastMessageId;
            }

            YT_TLOG_INFO("Asynchronously wrote messages to the queue")
                .With("Cluster", cluster.Name)
                .With("RequestCount", std::ssize(*requests))
                .With("SkippedPrefixCount", firstRequestIndex);
            ErrorState_->ClearError();
            written = true;
            break;
        } catch (const std::exception& ex) {
            auto clusterError = TError("Failed to write to the queue on cluster %Qv", cluster.Name).With(ex);
            YT_TLOG_WARNING("Failed to write to the queue on cluster")
                .With(clusterError);
            error.Add(clusterError);
            if (clusterError.FindMatching(NQueueClient::EErrorCode::ZombieEpoch) ||
                clusterError.FindMatching(NQueueClient::EErrorCode::InvalidEpoch))
            {
                FatalError_ = clusterError;
                ErrorState_->SetError(clusterError);
                requests->clear();
                return true;
            }

            if (std::ssize(Clusters_) > 1) {
                if (cluster.Session) {
                    cluster.Session.reset();
                    cluster.SessionFuture = {};
                    cluster.SessionOptions = {};
                } else {
                    cluster.SessionFuture = {};
                    cluster.SessionOptions.Retry = true;
                }
                CurrentClusterIndex_ = (CurrentClusterIndex_ + 1) % std::ssize(Clusters_);
                if (AtMostOnce_ && commitStarted) {
                    ErrorState_->SetError(clusterError);
                    YT_TLOG_WARNING("Dropping messages with unknown commit outcome before switching clusters")
                        .With("RequestCount", std::ssize(*requests));
                    requests->clear();
                    return true;
                }
            } else {
                cluster.Session.reset();
                cluster.SessionFuture = {};
                cluster.SessionOptions.Retry = true;
            }
        }
    }
    if (written) {
        for (auto& request : *requests) {
            auto onWritten = std::move(request.OnWritten);
            onWritten();
        }
        requests->clear();
        return true;
    }
    ErrorState_->SetError(error);
    return false;
}

void TAsyncQueueWriter::StartSession(TClusterData* cluster, const std::string& producerId)
{
    cluster->SessionOptions.MutationId = cluster->SessionOptions.GetOrGenerateMutationId();
    auto sessionId = NQueueClient::TQueueProducerSessionId(producerId);
    if (std::ssize(Clusters_) == 1) {
        cluster->SessionFuture = BIND(
            &TAsyncQueueWriter::CreateSessionWithRetries,
            MakeWeak(this),
            CurrentClusterIndex_,
            sessionId,
            cluster->SessionOptions)
            .AsyncVia(Context_->PoolInvoker)
            .Run();
    } else {
        cluster->SessionFuture = cluster->Client->CreateQueueProducerSession(
            Parameters_->ProducerPath,
            Parameters_->QueuePath,
            sessionId,
            cluster->SessionOptions);
    }
}

const TAsyncQueueWriter::TSessionResult& TAsyncQueueWriter::GetSession(
    TClusterData* cluster,
    const std::string& producerId)
{
    if (!cluster->Session) {
        if (!cluster->SessionFuture) {
            StartSession(cluster, producerId);
        }
        cluster->Session = NConcurrency::WaitFor(cluster->SessionFuture)
            .ValueOrThrow();
        cluster->PhysicalSequenceNumber = cluster->Session->SequenceNumber;
        cluster->LastMessageId = GetLastMessageIdFromUserMeta(cluster->Session->UserMeta);
    }
    return *cluster->Session;
}

TAsyncQueueWriter::TSessionResult TAsyncQueueWriter::CreateSessionWithRetries(
    TWeakPtr<TAsyncQueueWriter> weakThis,
    ssize_t clusterIndex,
    NQueueClient::TQueueProducerSessionId sessionId,
    NApi::TCreateQueueProducerSessionOptions options)
{
    std::optional<TBackoffStrategy> backoffStrategy;
    while (true) {
        auto strongThis = weakThis.Lock();
        if (!strongThis) {
            THROW_ERROR_EXCEPTION("Async queue writer was destroyed while creating producer session");
        }

        auto result = NConcurrency::WaitFor(strongThis->Clusters_[clusterIndex].Client->CreateQueueProducerSession(
            strongThis->Parameters_->ProducerPath,
            strongThis->Parameters_->QueuePath,
            sessionId,
            options));
        if (result.IsOK()) {
            strongThis->ErrorState_->ClearError();
            return result.Value();
        }

        strongThis->ErrorState_->SetError(TError("Failed to create queue producer session").With(result));
        auto minBackoff = strongThis->DynamicParameters_.Acquire()->BackoffDuration;
        strongThis.Reset();

        WaitBackoff(&backoffStrategy, minBackoff);
        options.Retry = true;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMessageIdQueueSinkState::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_id", &TThis::ProducerId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TAsyncQueueSink::TAsyncQueueSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , AtMostOnce_(GetParameters() && GetParameters()->AtMostOnceStrategy && GetParameters()->AtMostOnceStrategy->Enabled)
    , QueueSizeSemaphore_(AtMostOnce_
            ? New<NConcurrency::TAsyncSemaphore>(/*totalSlots*/ 0, /*enableOverdraft*/ true)
            : nullptr)
{
    if (AtMostOnce_) {
        ReconfigureAtMostOnce();
    }
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& /*dynamicContext*/) {
        if (Writer_) {
            Writer_->Reconfigure(GetDynamicWriterParameters());
        }
        if (AtMostOnce_) {
            ReconfigureAtMostOnce();
        }
    }));
}

TAsyncQueueSink::~TAsyncQueueSink()
{
    TerminateWriter();
}

void TAsyncQueueSink::DoTerminate()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    CurrentEpoch_.clear();
    TerminateWriter();
    NameTable_.Reset();
    TabletRouter_.Reset();
}

void TAsyncQueueSink::Init(IInitContextPtr initContext)
{
    if (GetSpec()->InputStreamIds.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected exactly one input stream id, but got %v",
            GetSpec()->InputStreamIds.size());
    }
    NameTable_ = GenerateNameTable();
    auto parameters = GetWriterParameters();
    TabletIndexColumn_ = IsTabletRoutingEnabled(*GetRoutingParameters())
        ? NameTable_->FindId(NTableClient::TabletIndexColumnName)
        : std::nullopt;
    TabletRouter_ = CreateTabletRouter(
        *GetRoutingParameters(),
        parameters->QueuePath,
        parameters->UpdatePartitionCountPeriod,
        GetContext()->StreamSpecStorage->GetSchema(*GetSpec()->InputStreamIds.begin()),
        GetContext(),
        Logger);
    if (TabletRouter_) {
        TabletRoutingErrorState_ = GetContext()->StatusProfiler->ErrorState("/tablet_routing");
    }

    FlowMetaColumn_ = parameters->WriteFlowQueueMeta
        ? NameTable_->FindId(parameters->FlowQueueMetaColumn)
        : std::nullopt;
    if (AtMostOnce_) {
        initContext->InitClient(AtMostOnceWatermark_, "v1/at_most_once_watermark");
    }
    Writer_ = CreateWriter();
    Writer_->Init(initContext, AtMostOnce_);
}

void TAsyncQueueSink::Distribute(
    const TOutputMessageConstPtr& message,
    TOnDistributedCallback onDistributed)
{
    if (AtMostOnce_) {
        ObserveEventLag(message->StreamId, message->EventTimestamp);
        onDistributed();
        if (message->MessageId > *AtMostOnceWatermark_) {
            CurrentEpoch_.push_back({message, {}});
        }
    } else {
        CurrentEpoch_.push_back({message, std::move(onDistributed)});
    }
}

void TAsyncQueueSink::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{
    Writer_->ThrowIfFatalError();
    if (AtMostOnce_ && !CurrentEpoch_.empty()) {
        *AtMostOnceWatermark_ = CurrentEpoch_.back().Message->MessageId;
    }
}

void TAsyncQueueSink::Commit()
{
    Writer_->ThrowIfFatalError();

    auto pending = std::exchange(CurrentEpoch_, {});
    if (pending.empty()) {
        return;
    }
    if (AtMostOnce_ && !QueueSizeSemaphore_->IsReady()) {
        YT_TLOG_WARNING("Async sink queue is full; dropping messages")
            .With("MessageCount", std::ssize(pending))
            .With("UsedBytes", QueueSizeSemaphore_->GetUsed())
            .With("TotalBytes", QueueSizeSemaphore_->GetTotal());
        return;
    }

    WriteMessages(std::move(pending));
}

void TAsyncQueueSink::WriteMessages(std::vector<TPendingMessage> pending)
{
    std::optional<TMessageId> initialMessageId;
    // A committed replay may route to a tablet removed by a queue shrink, so skip it before routing.
    if (TabletRouter_ && !AtMostOnce_ && Writer_->HasSingleCluster()) {
        initialMessageId = EnsureInitialMessageId();
    }

    ssize_t sentCount = 0;
    for (auto& item : pending) {
        if (!TryAdmitMessage(item, initialMessageId)) {
            continue;
        }
        auto row = BuildRow(item.Message);
        ++sentCount;
        if (AtMostOnce_) {
            auto guard = NConcurrency::TAsyncSemaphoreGuard::Acquire(QueueSizeSemaphore_, item.Message->ByteSize);
            bool withinLimit = QueueSizeSemaphore_->GetFree() >= 0;
            auto onWritten = BIND([guard = std::move(guard)] () mutable {
                guard.Release();
            });
            Writer_->Write(std::move(row), item.Message->MessageId, std::move(onWritten));
            if (!withinLimit) {
                break;
            }
            continue;
        }
        auto messageId = item.Message->MessageId;
        auto onWritten = BIND([weakThis = MakeWeak(this), item = std::move(item)] () mutable {
            if (auto strongThis = weakThis.Lock()) {
                YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(strongThis->GetContext()->SerializedInvoker);
                strongThis->ObserveEventLag(item.Message->StreamId, item.Message->EventTimestamp);
                item.OnDistributed();
            }
        });
        Writer_->Write(std::move(row), std::move(messageId), std::move(onWritten));
    }
    if (AtMostOnce_ && sentCount < std::ssize(pending)) {
        YT_TLOG_WARNING("Async sink queue size limit exceeded; dropping some messages")
            .With("DroppedMessageCount", std::ssize(pending) - sentCount)
            .With("TotalMessageCount", std::ssize(pending))
            .With("UsedBytes", QueueSizeSemaphore_->GetUsed())
            .With("TotalBytes", QueueSizeSemaphore_->GetTotal());
    }
}

i64 TAsyncQueueSink::GetTabletIndex(const TOutputMessageConstPtr& message) const
{
    // Keep the router alive if termination resets it while startup or retry backoff yields.
    auto router = TabletRouter_;
    std::optional<TBackoffStrategy> backoffStrategy;
    bool hadError = false;
    while (true) {
        try {
            router->Start();
            auto tabletIndex = router->GetTabletIndex(message->Payload);

            if (hadError) {
                TabletRoutingErrorState_->ClearError();
            }

            return tabletIndex;
        } catch (const std::exception& ex) {
            auto error = TError("Failed to evaluate queue tablet routing").With(TError(ex));
            if (error.FindMatching(NYT::EErrorCode::Canceled)) {
                throw;
            }

            TabletRoutingErrorState_->SetError(error);
            hadError = true;

            YT_TLOG_WARNING("Failed to evaluate queue tablet routing")
                .With(error);
        }

        WaitBackoff(&backoffStrategy, GetDynamicWriterParameters()->BackoffDuration);
    }
}

TMessageId TAsyncQueueSink::EnsureInitialMessageId()
{
    if (!InitialMessageId_) {
        InitialMessageId_ = NConcurrency::WaitFor(Writer_->GetInitialMessageId())
            .ValueOrThrow();
        LastSeenMessageId_ = *InitialMessageId_;
    }
    return *InitialMessageId_;
}

bool TAsyncQueueSink::TryAdmitMessage(
    TPendingMessage& item,
    const std::optional<TMessageId>& initialMessageId)
{
    if (AtMostOnce_) {
        return true;
    }
    if (initialMessageId && item.Message->MessageId <= *initialMessageId) {
        item.OnDistributed();
        return false;
    }
    if (item.Message->MessageId <= LastSeenMessageId_) {
        THROW_ERROR_EXCEPTION("Message IDs written to the async queue sink must strictly increase")
            .With("message_id", item.Message->MessageId)
            .With("last_message_id", LastSeenMessageId_);
    }
    LastSeenMessageId_ = item.Message->MessageId;
    return true;
}

NTableClient::TNameTablePtr TAsyncQueueSink::GenerateNameTable() const
{
    auto schema = GetContext()->StreamSpecStorage->GetSchema(*GetSpec()->InputStreamIds.begin());
    NTableClient::TNameTablePtr nameTable;
    if (GetParameters()->ColumnFilter) {
        nameTable = New<NTableClient::TNameTable>();
        for (const auto& field : *GetParameters()->ColumnFilter) {
            if (!schema->FindColumn(field)) {
                THROW_ERROR_EXCEPTION("Column %v not found in schema", field);
            }
            nameTable->RegisterNameOrThrow(field);
        }
    } else {
        nameTable = NTableClient::TNameTable::FromSchema(*schema);
    }
    if (GetParameters()->WriteFlowQueueMeta) {
        nameTable->RegisterNameOrThrow(GetParameters()->FlowQueueMetaColumn);
    }

    if (IsTabletRoutingEnabled(*GetRoutingParameters())) {
        nameTable->RegisterNameOrThrow(NTableClient::TabletIndexColumnName);
    }
    return nameTable;
}

NTableClient::TUnversionedOwningRow TAsyncQueueSink::BuildRow(const TOutputMessageConstPtr& message) const
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
    if (FlowMetaColumn_) {
        const auto meta = BuildFlowQueueMeta(*message);
        const auto serializedMeta = NYson::ConvertToYsonString(meta, NYson::EYsonFormat::Binary);
        builder.AddValue(NTableClient::MakeUnversionedAnyValue(serializedMeta.AsStringBuf(), *FlowMetaColumn_));
    }

    if (TabletRouter_) {
        builder.AddValue(NTableClient::MakeUnversionedInt64Value(
            GetTabletIndex(message),
            *TabletIndexColumn_));
    }

    return builder.FinishRow();
}

TAsyncQueueWriterParametersBasePtr TAsyncQueueSink::GetWriterParameters() const
{
    return DynamicPointerCast<TAsyncQueueWriterParametersBase>(GetParametersBase());
}

TQueueSinkTabletRoutingParametersPtr TAsyncQueueSink::GetRoutingParameters() const
{
    return DynamicPointerCast<TQueueSinkTabletRoutingParameters>(GetParametersBase());
}

TDynamicAsyncQueueWriterParametersPtr TAsyncQueueSink::GetDynamicWriterParameters() const
{
    return DynamicPointerCast<TDynamicAsyncQueueWriterParameters>(GetDynamicParametersBase());
}

TAsyncQueueWriterPtr TAsyncQueueSink::CreateWriter() const
{
    auto parameters = GetWriterParameters();
    return New<TAsyncQueueWriter>(
        GetContext(),
        parameters,
        std::vector<std::string>{*parameters->QueuePath.GetCluster()},
        GetDynamicWriterParameters(),
        NameTable_,
        GetContext()->StatusProfiler->ErrorState("/async_queue_writer"),
        Logger);
}

TAsyncQueueWriterPtr TAsyncMultiClusterQueueSink::CreateWriter() const
{
    std::vector<std::string> clusters;
    if (auto cluster = GetParameters()->QueuePath.GetCluster()) {
        clusters.push_back(*cluster);
    } else if (GetParameters()->UseClusters && GetParameters()->QueuePath.GetClusters()) {
        clusters = *GetParameters()->QueuePath.GetClusters();
    }
    return New<TAsyncQueueWriter>(
        GetContext(),
        GetParameters(),
        std::move(clusters),
        GetDynamicParameters(),
        NameTable_,
        GetContext()->StatusProfiler->ErrorState("/async_multi_cluster_queue_writer"),
        Logger);
}

void TAsyncQueueSink::ReconfigureAtMostOnce()
{
    auto parameters = GetDynamicParameters()->AtMostOnceStrategy;
    THROW_ERROR_EXCEPTION_UNLESS(parameters, "AtMostOnceStrategy is required with at most once strategy");
    QueueSizeSemaphore_->SetTotal(parameters->TotalQueueBytesLimit);
}

void TAsyncQueueSink::TerminateWriter()
{
    if (!Writer_) {
        return;
    }
    auto duration = AtMostOnce_
        ? GetDynamicParameters()->AtMostOnceStrategy->SuspendDestructionDuration
        : TDuration::Zero();
    BIND([
        writer = std::exchange(Writer_, {}),
        semaphore = QueueSizeSemaphore_,
        duration,
        invoker = GetContext()->SerializedInvoker,
        Logger = Logger] {
        if (duration == TDuration::Zero() || semaphore->IsFree()) {
            writer->Terminate();
            return;
        }

        YT_TLOG_INFO("Waiting for in-flight async queue writes during termination")
            .With("SuspendDurationMs", duration.MilliSeconds())
            .With("UsedBytes", semaphore->GetUsed());
        auto acquireFuture = semaphore->AsyncAcquire(semaphore->GetTotal());
        auto timeoutFuture = NConcurrency::TDelayedExecutor::MakeDelayed(duration);
        AnySucceeded(
            std::vector<TFuture<void>>{acquireFuture.AsVoid(), timeoutFuture},
            TFutureCombinerOptions{
                .PropagateCancelationToInput = false,
                .CancelInputOnShortcut = false,
            })
            .Subscribe(BIND([
                Logger = Logger,
                acquireFuture,
                timeoutFuture,
                writer] (const TError& error) {
                if (!error.IsOK()) {
                    YT_TLOG_WARNING("Error while waiting for async queue writes during termination")
                        .With(error);
                }
                if (!acquireFuture.IsSet()) {
                    YT_TLOG_WARNING("Termination timeout expired with async queue writes still in flight");
                }
                auto cancelError = TError(NYT::EErrorCode::Canceled, "Async queue writer terminated");
                acquireFuture.Cancel(cancelError);
                timeoutFuture.Cancel(cancelError);
                writer->Terminate();
            }).Via(invoker));
    })
        .Via(GetContext()->SerializedInvoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TError CheckHeartbeatCommitOrdering(const NApi::IClientPtr& client, const NYPath::TYPath& path)
{
    try {
        NApi::TGetNodeOptions options;
        options.Attributes = {"type", "commit_ordering"};
        auto node = NYTree::ConvertToNode(NConcurrency::WaitFor(client->GetNode(path, options))
                .ValueOrThrow());
        const auto& attributes = node->Attributes();
        // Ordered replicas of a chaos replicated table are always strong; the table itself has no commit ordering.
        if (attributes.Get<NObjectClient::EObjectType>("type") == NObjectClient::EObjectType::ChaosReplicatedTable) {
            return {};
        }

        auto commitOrdering = attributes.Get<NTransactionClient::ECommitOrdering>("commit_ordering");
        if (commitOrdering == NTransactionClient::ECommitOrdering::Weak) {
            return TError(
                "Queue heartbeats to %v with weak commit ordering are unreliable: heartbeats may overtake data rows",
                path)
                .With("commit_ordering", commitOrdering);
        }

        return {};
    } catch (const std::exception& ex) {
        return TError("Failed to check heartbeat commit ordering of queue %v", path)
            .With(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

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
        GetContext()->Logger.WithTag("QueuePath", GetParameters()->QueuePath),
        GetContext()->StatusProfiler->WithPrefix("/queue_info")))
    , HeartbeatExecutor_(New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TQueueSinkController::WriteFlowQueueMeta, MakeWeak(this)),
        NConcurrency::TPeriodicExecutorOptions::WithJitter(GetDynamicParameters()->FlowQueueMetaHeartbeatPeriod)))
    , WriteMetaErrorState_(GetContext()->StatusProfiler->ErrorState("write_meta"))
    , HeartbeatOrderingErrorState_(GetContext()->StatusProfiler->ErrorState("/heartbeat_ordering"))
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
    HeartbeatOrderingErrorState_->SetError(CheckHeartbeatCommitOrdering(Client_, GetParameters()->QueuePath.GetPath()));
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
        auto error = TError("Failed to write flow queue meta heartbeat").With(ex);
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
    , HeartbeatOrderingErrorState_(GetContext()->StatusProfiler->ErrorState("/heartbeat_ordering"))
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
        GetContext()->Logger.WithTag("QueuePath", GetParameters()->QueuePath).WithTag("Cluster", cluster),
        GetContext()->StatusProfiler->WithPrefix(Format("/queue_info/clusters/%v", cluster)));
}

void TMultiClusterQueueSinkController::Init(IInitContextPtr initContext)
{
    for (const auto& [cluster, info] : InfoByCluster_) {
        info->Init(initContext->WithPrefix("queue_info_" + cluster));
    }

    if (GetParameters()->WriteFlowQueueMeta) {
        HeartbeatExecutor_->Start();
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
    TError orderingError;
    for (const auto& [cluster, client] : ClientByCluster_) {
        auto error = CheckHeartbeatCommitOrdering(client, GetParameters()->QueuePath.GetPath());
        if (!error.IsOK()) {
            if (orderingError.IsOK()) {
                orderingError = TError("Queue heartbeat commit ordering is unsafe or could not be checked");
            }

            auto clusterError = std::move(error)
                .With("cluster", cluster);
            orderingError = std::move(orderingError)
                .With(std::move(clusterError));
        }
    }

    HeartbeatOrderingErrorState_->SetError(orderingError);
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

        std::vector<TError> clusterErrors;
        for (const auto& [cluster, info] : InfoByCluster_) {
            auto partitions = info->GetPartitionCount();
            if (!partitions) {
                YT_TLOG_WARNING("Partition count is not available for cluster, skipping heartbeat on this cluster")
                    .With("Cluster", cluster);
                continue;
            }

            // Clusters are independent: a failure on one must not block heartbeats on the others.
            try {
                const auto& client = GetOrCrash(ClientByCluster_, cluster);

                auto transaction = NConcurrency::WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet, NApi::TTransactionStartOptions()))
                    .ValueOrThrow();

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
            } catch (const std::exception& ex) {
                clusterErrors.push_back(TError("Failed to write flow queue meta heartbeat to cluster %Qv", cluster)
                        .With(ex));
            }
        }

        if (!clusterErrors.empty()) {
            THROW_ERROR TError("Failed to write flow queue meta heartbeat to some clusters")
                .With(std::move(clusterErrors));
        }

        WriteMetaErrorState_->ClearError();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to write flow queue meta heartbeat").With(ex);
        // TODO: pass to public controller log.
        WriteMetaErrorState_->SetError(error);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
