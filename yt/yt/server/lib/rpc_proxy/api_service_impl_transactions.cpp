#include "api_service_impl.h"

#include <yt/yt/ytlib/transaction_client/clock_manager.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

namespace NYT::NRpcProxy {

using namespace NApi::NRpcProxy;
using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSignature;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void TMasterMetadataApiService::RegisterTransactionMethods()
{
    RegisterApiMethod(
        EMultiproxyMethodKind::Write,
        RPC_SERVICE_METHOD_DESC(GenerateTimestamps)
            .SetInvokerProvider(BIND(&TMasterMetadataApiService::GetGenerateTimestampsInvoker, Unretained(this))));

    RegisterApiMethod(
        EMultiproxyMethodKind::Write,
        RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetInvokerProvider(BIND(&TMasterMetadataApiService::GetStartTransactionInvoker, Unretained(this))));

    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(PingTransaction));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AbortTransaction));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(CommitTransaction));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(FlushTransaction));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(AttachTransaction));
    RegisterApiMethod(EMultiproxyMethodKind::Write, RPC_SERVICE_METHOD_DESC(DetachTransaction));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, GenerateTimestamps)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto count = request->count();
    auto clockClusterTag = request->has_clock_cluster_tag()
        ? FromProto<TCellTag>(request->clock_cluster_tag())
        : InvalidCellTag;

    context->AnnotateRequest()
        .With("Count", count)
        .With("ClockClusterTag", clockClusterTag);

    auto connection = client->GetNativeConnection();
    if (clockClusterTag == InvalidCellTag) {
        connection->GetClockManager()->ValidateDefaultClock("Unable to generate timestamps");
    }

    ExecuteCall(
        context,
        [
            connection = std::move(connection),
            clockClusterTag,
            count,
            Logger = Logger
        ] {
            const auto& timestampProvider = connection->GetTimestampProvider();
            return timestampProvider->GenerateTimestamps(count, clockClusterTag)
                .AsUnique()
                .Apply(BIND([connection, clockClusterTag, count, Logger] (TErrorOr<TTimestamp>&& providerResult) {
                    if (providerResult.IsOK() ||
                        !(providerResult.FindMatching(NTransactionClient::EErrorCode::UnknownClockClusterTag) ||
                            providerResult.FindMatching(NTransactionClient::EErrorCode::ClockClusterTagMismatch) ||
                            providerResult.FindMatching(NRpc::EErrorCode::UnsupportedServerFeature)))
                    {
                        return MakeFuture(std::move(providerResult));
                    }

                    YT_TLOG_WARNING("Wrong clock cluster tag, trying to generate timestamps via direct call")
                        .With("ClockClusterTag", clockClusterTag)
                        .With(providerResult);

                    auto alienClient = connection->GetClockManager()->GetTimestampProviderOrThrow(clockClusterTag);
                    return alienClient->GenerateTimestamps(count);
                }));
        },
        [clockClusterTag] (const auto& context, const TTimestamp& timestamp) {
            auto* response = &context->Response();
            response->set_timestamp(ToProto(timestamp));

            context->AnnotateResponse()
                .WithFormat("Timestamp", "%v@%v", timestamp, clockClusterTag);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, StartTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    if (!request->sticky() && request->type() == NApi::NRpcProxy::NProto::ETransactionType::TT_TABLET) {
        THROW_ERROR_EXCEPTION("Tablet transactions must be sticky");
    }

    auto transactionType = FromProto<NTransactionClient::ETransactionType>(request->type());

    TTransactionStartOptions options;
    SetMutatingOptions(&options, request, context.Get());
    if (request->has_timeout()) {
        options.Timeout = FromProto<TDuration>(request->timeout());
    }
    if (request->has_deadline()) {
        options.Deadline = FromProto<TInstant>(request->deadline());
    }
    if (request->has_id()) {
        FromProto(&options.Id, request->id());
    }
    if (request->has_parent_id()) {
        FromProto(&options.ParentId, request->parent_id());
    }
    if (request->has_replicate_to_master_cell_tags()) {
        options.ReplicateToMasterCellTags =
            FromProto<TCellTagList>(request->replicate_to_master_cell_tags().cell_tags());
    }
    options.AutoAbort = false;
    options.Sticky = request->sticky();
    options.Ping = request->ping();
    options.PingAncestors = request->ping_ancestors();
    options.PingerAddress = context->GetEndpointDescription();
    options.Atomicity = FromProto<NTransactionClient::EAtomicity>(request->atomicity());
    options.Durability = FromProto<NTransactionClient::EDurability>(request->durability());
    if (request->has_attributes()) {
        options.Attributes = NYTree::FromProto(request->attributes());
    }
    options.PrerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
    if (request->has_start_timestamp()) {
        options.StartTimestamp = FromProto<NTransactionClient::TTimestamp>(request->start_timestamp());
    }

    context->AnnotateRequest()
        .With("TransactionType", transactionType)
        .With("TransactionId", options.Id)
        .With("ParentId", options.ParentId)
        .With("PrerequisiteTransactionIds", options.PrerequisiteTransactionIds)
        .With("Timeout", options.Timeout)
        .With("Deadline", options.Deadline)
        .With("AutoAbort", options.AutoAbort)
        .With("Sticky", options.Sticky)
        .With("Ping", options.Ping)
        .With("PingAncestors", options.PingAncestors)
        .With("Atomicity", options.Atomicity)
        .With("Durability", options.Durability)
        .With("StartTimestamp", options.StartTimestamp);

    ExecuteCall(
        context,
        [=] {
            return client->StartTransaction(transactionType, options);
        },
        [=, this, this_ = MakeStrong(this)] (const auto& context, const auto& transaction) {
            auto* response = &context->Response();
            ToProto(response->mutable_id(), transaction->GetId());
            response->set_start_timestamp(ToProto(transaction->GetStartTimestamp()));
            if (transactionType == ETransactionType::Tablet) {
                response->set_sequence_number_source_id(NextSequenceNumberSourceId_++);
            }

            if (options.Sticky) {
                StickyTransactionPool_->RegisterTransaction(transaction);
            }

            context->AnnotateResponse()
                .With("TransactionId", transaction->GetId())
                .With("StartTimestamp", transaction->GetStartTimestamp());
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, PingTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    TTransactionAttachOptions attachOptions = {};
    attachOptions.Ping = false;
    attachOptions.PingAncestors = request->ping_ancestors();
    attachOptions.PingerAddress = context->GetEndpointDescription();

    context->AnnotateRequest()
        .With("TransactionId", transactionId);

    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        attachOptions);

    ExecuteCall(
        context,
        [=] {
            return transaction->Ping();
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, CommitTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    TTransactionCommitOptions options;
    SetMutatingOptions(&options, request, context.Get());
    options.AdditionalParticipantCellIds = FromProto<std::vector<TCellId>>(request->additional_participant_cell_ids());
    options.ExpectedPrepareSignatures = FromProto<std::vector<TTransactionSignature>>(request->expected_prepare_signatures());
    // COMPAT(atalmenev): old clients don't send expected_prepare_signatures.
    if (options.ExpectedPrepareSignatures.empty()) {
        options.ExpectedPrepareSignatures.assign(
            options.AdditionalParticipantCellIds.size(),
            FinalTransactionSignature);
    }
    if (options.ExpectedPrepareSignatures.size() != options.AdditionalParticipantCellIds.size()) {
        THROW_ERROR_EXCEPTION("Expected prepare signatures count mismatch")
            .With("additional_participant_cell_ids_size", options.AdditionalParticipantCellIds.size())
            .With("expected_prepare_signatures_size", options.ExpectedPrepareSignatures.size());
    }
    if (request->has_max_allowed_commit_timestamp()) {
        options.MaxAllowedCommitTimestamp = FromProto<NTransactionClient::TTimestamp>(request->max_allowed_commit_timestamp());
    }
    if (request->has_prerequisite_options()) {
        FromProto(&options, request->prerequisite_options());
    }

    context->AnnotateRequest()
        .With("TransactionId", transactionId)
        .With("AdditionalParticipantCellIds", options.AdditionalParticipantCellIds)
        .With("PrerequisiteTransactionIds", options.PrerequisiteTransactionIds);

    TTransactionAttachOptions attachOptions = {};
    attachOptions.Ping = false;
    attachOptions.PingAncestors = false;
    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        attachOptions);

    ExecuteCall(
        context,
        [=] {
            return transaction->Commit(options);
        },
        [] (const auto& context, const TTransactionCommitResult& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_commit_timestamps(), result.CommitTimestamps);
            response->set_primary_commit_timestamp(ToProto(result.PrimaryCommitTimestamp));

            context->AnnotateResponse()
                .With("PrimaryCommitTimestamp", result.PrimaryCommitTimestamp)
                .With("CommitTimestamps", result.CommitTimestamps);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, FlushTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    context->AnnotateRequest()
        .With("TransactionId", transactionId);

    TTransactionAttachOptions attachOptions = {};
    attachOptions.Ping = false;
    attachOptions.PingAncestors = false;
    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        attachOptions);

    ExecuteCall(
        context,
        [=] {
            return transaction->Flush();
        },
        [&] (const auto& context, const TTransactionFlushResult& result) {
            auto* response = &context->Response();
            ToProto(response->mutable_participant_cell_ids(), result.ParticipantCellIds);
            ToProto(response->mutable_expected_prepare_signatures(), result.ExpectedPrepareSignatures);

            context->AnnotateResponse()
                .With("ParticipantCellIds", result.ParticipantCellIds);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, AbortTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    TTransactionAbortOptions options;
    SetMutatingOptions(&options, request, context.Get());

    context->AnnotateRequest()
        .With("TransactionId", transactionId);

    TTransactionAttachOptions attachOptions = {};
    attachOptions.Ping = false;
    attachOptions.PingAncestors = false;
    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        attachOptions);

    ExecuteCall(
        context,
        [=] {
            return transaction->Abort(options);
        });
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, AttachTransaction)
{
    auto client = GetAuthenticatedClientOrThrow(context, request);

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());
    TTransactionAttachOptions options;
    if (request->has_ping_period()) {
        options.PingPeriod = TDuration::FromValue(request->ping_period());
    }
    if (request->has_ping()) {
        options.Ping = request->ping();
    }
    if (request->has_ping_ancestors()) {
        options.PingAncestors = request->ping_ancestors();
    }

    context->AnnotateRequest()
        .With("TransactionId", transactionId);

    auto transaction = GetTransactionOrThrow(
        client,
        transactionId,
        options,
        /*searchInPool*/ true);

    response->set_type(static_cast<NApi::NRpcProxy::NProto::ETransactionType>(transaction->GetType()));
    response->set_start_timestamp(ToProto(transaction->GetStartTimestamp()));
    response->set_atomicity(static_cast<NApi::NRpcProxy::NProto::EAtomicity>(transaction->GetAtomicity()));
    response->set_durability(static_cast<NApi::NRpcProxy::NProto::EDurability>(transaction->GetDurability()));
    response->set_timeout(ToProto(transaction->GetTimeout()));
    if (transaction->GetType() == ETransactionType::Tablet) {
        response->set_sequence_number_source_id(NextSequenceNumberSourceId_++);
    }

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMasterMetadataApiService, DetachTransaction)
{
    auto transactionId = FromProto<TTransactionId>(request->transaction_id());

    context->AnnotateRequest()
        .With("TransactionId", transactionId);

    StickyTransactionPool_->UnregisterTransaction(transactionId);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
