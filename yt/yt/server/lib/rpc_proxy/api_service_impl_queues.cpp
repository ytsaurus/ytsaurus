#include "api_service_impl.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueueClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TApiService::RegisterQueueMethods(TMultiproxyMethodList* methodList)
{
    auto registerMethod = [&] (EMultiproxyMethodKind methodKind, TMethodDescriptor&& descriptor) {
        RegisterMethodForMultiproxy(methodList, methodKind, descriptor);
    };

    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AdvanceConsumer));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AdvanceQueueConsumer));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(PullQueue));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(PullConsumer));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(PullQueueConsumer));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RegisterQueueConsumer));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(UnregisterQueueConsumer));
    registerMethod(EMultiproxyMethodKind::Read, RPC_SERVICE_METHOD_DESC(ListQueueConsumerRegistrations));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CreateQueueProducerSession));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(RemoveQueueProducerSession));
    registerMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PushQueueProducer));
}

////////////////////////////////////////////////////////////////////////////////

void TApiService::ProcessPullQueueDetailedProfilingInfo(
    TWallTimer timer,
    const std::string& userTag,
    const TDetailedProfilingInfoPtr& detailedProfilingInfo)
{
    TDetailedProfilingCountersPtr counters;
    if (detailedProfilingInfo->EnableDetailedTableProfiling) {
        counters = GetOrCreateDetailedProfilingCounters({
            .UserTag = userTag,
            .TablePath = detailedProfilingInfo->TablePath,
        });

        counters->PullQueueDurationTimer().Record(timer.GetElapsedTime());
        counters->PullQueueMountCacheWaitTimer().Record(detailedProfilingInfo->MountCacheWaitTime);
        counters->PullQueuePermissionCacheWaitTimer().Record(detailedProfilingInfo->PermissionCacheWaitTime);
    } else if (!detailedProfilingInfo->RetryReasons.empty()) {
        counters = GetOrCreateDetailedProfilingCounters({});
    }

    for (const auto& reason : detailedProfilingInfo->RetryReasons) {
        counters->GetRetryCounterByReason(reason)->Increment();
    }
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PushQueueProducer)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    auto producerPath = FromProto<TRichYPath>(request->producer_path());
    auto queuePath = FromProto<TRichYPath>(request->queue_path());

    auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

    TPushQueueProducerOptions options;
    SetTimeoutOptions(&options, context.Get());
    options.SequenceNumber = YT_OPTIONAL_FROM_PROTO(*request, sequence_number, TQueueProducerSequenceNumber);
    if (request->has_require_sync_replica()) {
        options.RequireSyncReplica = request->require_sync_replica();
    }

    if (request->has_user_meta()) {
        options.UserMeta = ConvertToNode(TYsonStringBuf(request->user_meta()));
    }

    context->AnnotateRequest()
        .With("ProducerPath", producerPath)
        .With("QueuePath", queuePath)
        .With("SessionId", sessionId)
        .With("Epoch", request->epoch())
        .With("RequireSyncReplica", options.RequireSyncReplica)
        .With("TransactionId", transactionId);

    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        /*options*/ std::nullopt,
        /*searchInPool*/ true);

    auto format = GetFormat(context, request);

    auto tableMountCache = client->GetTableMountCache();
    auto queueTableInfoFuture = tableMountCache->GetTableInfo(queuePath.GetPath());
    auto queueTableInfo = WaitFor(queueTableInfoFuture)
        .ValueOrThrow("Path %v does not point to a valid queue", queuePath);

    auto rowset = DeserializeRowset(
        request->rowset_descriptor(),
        queueTableInfo->Schemas[ETableSchemaKind::WriteViaQueueProducer],
        format,
        MergeRefsToRef<TApiServiceBufferTag>(request->Attachments()),
        Logger);

    ExecuteCall(
        context,
        [
            =,
            producerPath = std::move(producerPath),
            queuePath = std::move(queuePath),
            sessionId = std::move(sessionId),
            rowset = std::move(rowset),
            options = std::move(options)
        ] {
            auto rowsetRows = rowset->GetRows();

            return transaction->PushQueueProducer(
                producerPath,
                queuePath,
                sessionId,
                FromProto<TQueueProducerEpoch>(request->epoch()),
                rowset->GetNameTable(),
                rowsetRows,
                options);
        },
        [] (const auto& context, const auto& pushQueueProducerResult) {
            auto* response = &context->Response();
            response->set_last_sequence_number(ToProto(pushQueueProducerResult.LastSequenceNumber));
            response->set_skipped_row_count(pushQueueProducerResult.SkippedRowCount);

            context->AnnotateResponse()
                .With("LastSequenceNumber", pushQueueProducerResult.LastSequenceNumber.Underlying())
                .With("SkippedRowCount", pushQueueProducerResult.SkippedRowCount);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AdvanceQueueConsumer)
{
    AdvanceQueueConsumerImpl(request, response, context);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, AdvanceConsumer)
{
    AdvanceQueueConsumerImpl(request, response, context);
}

void TApiService::AdvanceQueueConsumerImpl(
    NApi::NRpcProxy::NProto::TReqAdvanceQueueConsumer* request,
    NApi::NRpcProxy::NProto::TRspAdvanceQueueConsumer* /*response*/,
    const TCtxAdvanceQueueConsumerPtr& context)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
    auto queuePath = FromProto<TRichYPath>(request->queue_path());

    TAdvanceQueueConsumerOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto oldOffset = YT_OPTIONAL_FROM_PROTO(*request, old_offset);
    context->AnnotateRequest()
        .With("ConsumerPath", consumerPath)
        .With("QueuePath", queuePath)
        .With("PartitionIndex", request->partition_index())
        .With("OldOffset", oldOffset)
        .With("NewOffset", request->new_offset())
        .With("TransactionId", transactionId);

    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        /*options*/ std::nullopt,
        /*searchInPool*/ true);

    ExecuteCall(
        context,
        [=, consumerPath = std::move(consumerPath), queuePath = std::move(queuePath)] {
            return transaction->AdvanceQueueConsumer(
                consumerPath,
                queuePath,
                request->partition_index(),
                oldOffset,
                request->new_offset(),
                options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PullQueue)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TWallTimer timer;

    auto queuePath = FromProto<TRichYPath>(request->queue_path());

    TPullQueueOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
    options.DetailedProfilingInfo = detailedProfilingInfo;

    auto rowBatchReadOptions = FromProto<NQueueClient::TQueueRowBatchReadOptions>(request->row_batch_read_options());

    context->AnnotateRequest()
        .With("QueuePath", queuePath)
        .With("Offset", request->offset())
        .With("PartitionIndex", request->partition_index())
        .With("MaxRowCount", rowBatchReadOptions.MaxRowCount)
        .With("MaxDataWeight", rowBatchReadOptions.MaxDataWeight)
        .With("DataWeightPerRowHint", rowBatchReadOptions.DataWeightPerRowHint);

    // TODO(achulkov2): Support WorkloadDescriptor.
    options.UseNativeTabletNodeApi = request->use_native_tablet_node_api();
    if (request->has_replica_consistency()) {
        options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
    }

    ExecuteCall(
        context,
        [=] {
            return client->PullQueue(
                queuePath,
                request->offset(),
                request->partition_index(),
                rowBatchReadOptions,
                options);
        },
        [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
        (const auto& context, const auto& result) {
            const auto& queueRowset = result.Rowset;
            auto* response = &context->Response();
            response->Attachments() = PrepareRowsetForAttachment(response, static_cast<IUnversionedRowsetPtr>(queueRowset));
            response->set_start_offset(queueRowset->GetStartOffset());

            ProcessPullQueueDetailedProfilingInfo(
                timer,
                context->GetAuthenticationIdentity().UserTag,
                detailedProfilingInfo);

            context->AnnotateResponse()
                .With("RowCount", queueRowset->GetRows().size())
                .With("StartOffset", queueRowset->GetStartOffset())
                .With("EnableDetailedTableProfiling", detailedProfilingInfo->EnableDetailedTableProfiling);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PullQueueConsumer)
{
    PullQueueConsumerImpl(request, response, context);
}

DEFINE_RPC_SERVICE_METHOD(TApiService, PullConsumer)
{
    PullQueueConsumerImpl(request, response, context);
}

void TApiService::PullQueueConsumerImpl(
    NApi::NRpcProxy::NProto::TReqPullQueueConsumer* request,
    NApi::NRpcProxy::NProto::TRspPullQueueConsumer* /*response*/,
    const TCtxPullQueueConsumerPtr& context)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    TWallTimer timer;

    auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
    auto queuePath = FromProto<TRichYPath>(request->queue_path());

    TPullQueueConsumerOptions options;
    SetTimeoutOptions(&options, context.Get());

    auto detailedProfilingInfo = New<TDetailedProfilingInfo>();
    options.DetailedProfilingInfo = detailedProfilingInfo;

    auto rowBatchReadOptions = FromProto<NQueueClient::TQueueRowBatchReadOptions>(request->row_batch_read_options());

    std::optional<i64> offset = YT_OPTIONAL_FROM_PROTO(*request, offset);

    context->AnnotateRequest()
        .With("ConsumerPath", consumerPath)
        .With("QueuePath", queuePath)
        .With("Offset", offset)
        .With("PartitionIndex", request->partition_index())
        .With("MaxRowCount", rowBatchReadOptions.MaxRowCount)
        .With("MaxDataWeight", rowBatchReadOptions.MaxDataWeight)
        .With("DataWeightPerRowHint", rowBatchReadOptions.DataWeightPerRowHint);

    // TODO(achulkov2): Support WorkloadDescriptor.
    if (request->has_replica_consistency()) {
        options.ReplicaConsistency = FromProto<EReplicaConsistency>(request->replica_consistency());
    }

    ExecuteCall(
        context,
        [=] {
            return client->PullQueueConsumer(
                consumerPath,
                queuePath,
                offset,
                request->partition_index(),
                rowBatchReadOptions,
                options);
        },
        [=, this, this_ = MakeStrong(this), detailedProfilingInfo = std::move(detailedProfilingInfo)]
        (const auto& context, const auto& result) {
            const auto& queueRowset = result.Rowset;
            auto* response = &context->Response();
            response->Attachments() = PrepareRowsetForAttachment(response, static_cast<IUnversionedRowsetPtr>(queueRowset));
            response->set_start_offset(queueRowset->GetStartOffset());

            ProcessPullQueueDetailedProfilingInfo(
                timer,
                context->GetAuthenticationIdentity().UserTag,
                detailedProfilingInfo);

            context->AnnotateResponse()
                .With("RowCount", queueRowset->GetRows().size())
                .With("StartOffset", queueRowset->GetStartOffset())
                .With("EnableDetailedTableProfiling", detailedProfilingInfo->EnableDetailedTableProfiling);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RegisterQueueConsumer)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto queuePath = FromProto<TRichYPath>(request->queue_path());
    auto consumerPath = FromProto<TRichYPath>(request->consumer_path());
    bool vital = request->vital();

    TRegisterQueueConsumerOptions options;
    SetTimeoutOptions(&options, context.Get());
    if (request->has_partitions()) {
        options.Partitions = FromProto<std::vector<int>>(request->partitions().items());
    }

    context->AnnotateRequest()
        .With("QueuePath", queuePath)
        .With("ConsumerPath", consumerPath)
        .With("Vital", vital)
        .With("Partitions", options.Partitions);

    ExecuteCall(
        context,
        [=] {
            return client->RegisterQueueConsumer(
                queuePath,
                consumerPath,
                vital,
                options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, UnregisterQueueConsumer)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto queuePath = FromProto<TRichYPath>(request->queue_path());
    auto consumerPath = FromProto<TRichYPath>(request->consumer_path());

    TUnregisterQueueConsumerOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("QueuePath", queuePath)
        .With("ConsumerPath", consumerPath);

    ExecuteCall(
        context,
        [=] {
            return client->UnregisterQueueConsumer(
                queuePath,
                consumerPath,
                options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, ListQueueConsumerRegistrations)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    std::optional<TRichYPath> queuePath;
    if (request->has_queue_path()) {
        queuePath = FromProto<TRichYPath>(request->queue_path());
    }
    std::optional<TRichYPath> consumerPath;
    if (request->has_consumer_path()) {
        consumerPath = FromProto<TRichYPath>(request->consumer_path());
    }

    TListQueueConsumerRegistrationsOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("QueuePath", queuePath)
        .With("ConsumerPath", consumerPath);

    ExecuteCall(
        context,
        [=] {
            return client->ListQueueConsumerRegistrations(
                queuePath,
                consumerPath,
                options);
        },
        [=] (const auto& context, const std::vector<TListQueueConsumerRegistrationsResult>& registrations) {
            auto* response = &context->Response();
            for (const auto& registration : registrations) {
                auto* protoRegistration = response->add_registrations();
                ToProto(protoRegistration->mutable_queue_path(), registration.QueuePath);
                ToProto(protoRegistration->mutable_consumer_path(), registration.ConsumerPath);
                protoRegistration->set_vital(registration.Vital);
                if (registration.Partitions) {
                    ToProto(protoRegistration->mutable_partitions()->mutable_items(), *registration.Partitions);
                }
            }

            context->AnnotateResponse()
                .With("Registrations", registrations.size());
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, CreateQueueProducerSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto producerPath = FromProto<TRichYPath>(request->producer_path());
    auto queuePath = FromProto<TRichYPath>(request->queue_path());
    auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

    TCreateQueueProducerSessionOptions options;
    SetTimeoutOptions(&options, context.Get());
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_user_meta()) {
        options.UserMeta = ConvertToNode(TYsonStringBuf(request->user_meta()));
    }

    context->AnnotateRequest()
        .With("ProducerPath", producerPath)
        .With("QueuePath", queuePath)
        .With("SessionId", sessionId)
        .With("MutationId", options.MutationId);

    ExecuteCall(
        context,
        [=] {
            return client->CreateQueueProducerSession(
                producerPath,
                queuePath,
                sessionId,
                options);
        },
        [=] (const auto& context, const TCreateQueueProducerSessionResult& result) {
            auto* response = &context->Response();

            response->set_sequence_number(ToProto(result.SequenceNumber));
            response->set_epoch(ToProto(result.Epoch));
            if (result.UserMeta) {
                ToProto(response->mutable_user_meta(), ConvertToYsonString(result.UserMeta).ToString());
            }

            context->AnnotateResponse()
                .With("SequenceNumber", result.SequenceNumber)
                .With("Epoch", result.Epoch);
        });
}

DEFINE_RPC_SERVICE_METHOD(TApiService, RemoveQueueProducerSession)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto producerPath = FromProto<TRichYPath>(request->producer_path());
    auto queuePath = FromProto<TRichYPath>(request->queue_path());
    auto sessionId = FromProto<TQueueProducerSessionId>(request->session_id());

    TRemoveQueueProducerSessionOptions options;
    SetTimeoutOptions(&options, context.Get());

    context->AnnotateRequest()
        .With("ProducerPath", producerPath)
        .With("QueuePath", queuePath)
        .With("SessionId", sessionId);

    ExecuteCall(
        context,
        [=] {
            return client->RemoveQueueProducerSession(
                producerPath,
                queuePath,
                sessionId,
                options);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
