#pragma once
#ifndef OBJECT_SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include object_service.h"
// For the sake of sane code completion.
#include "object_service.h"
#endif

#include "config.h"
#include "private.h"
#include "profiling.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/service_detail.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/access_control/cast.h>
#include <yt/yt/orm/server/access_control/helpers.h>

#include <yt/yt/orm/server/objects/attribute_matcher.h>
#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/cast.h>
#include <yt/yt/orm/server/objects/config.h>
#include <yt/yt/orm/server/objects/db_config.h>
#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/proto_values_consumer.h>
#include <yt/yt/orm/server/objects/transaction.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/transaction_wrapper.h>
#include <yt/yt/orm/server/objects/type_handler.h>
#include <yt/yt/orm/server/objects/watch_query_executor.h>
#include <yt/yt/orm/server/objects/request_handler.h>

#include <yt/yt/orm/client/native/public.h>
#include <yt/yt/orm/client/objects/transaction_context.h>
#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/orm/library/attributes/merge_attributes.h>
#include <yt/yt/orm/library/attributes/ytree.h>

#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/string_merger.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <google/protobuf/timestamp.pb.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Extend the list consciously.
using namespace NObjects;
using ::NYT::FromProto;
using ::NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void MaybeFlushPerformanceStatistics(const TTransactionPtr& transaction, auto* response, bool doFlush)
{
    if (doFlush) {
        ToProto(response->mutable_performance_statistics(), transaction->FlushPerformanceStatistics());
    }
}

void FillPerformanceStatistics(auto* protoStatistics, const std::optional<TPerformanceStatistics>& statistic)
{
    if (statistic) {
        ToProto(protoStatistics, *statistic);
        auto traceContext = NTracing::TryGetCurrentTraceContext();
        if (traceContext && traceContext->IsRecorded()) {
            traceContext->AddTag("read_phase_count", statistic->ReadPhaseCount);
        }
    }
}

void FillCommitTimestamp(auto* response, std::optional<TTimestamp> timestamp)
{
    if (timestamp.has_value()) {
        response->set_commit_timestamp(timestamp.value());
    }
}

template <
    class TObjectServiceProtoModule,
    class TObjectTypeRemapper>
class TObjectService
    : public NMaster::TServiceBase
{
public:
    TObjectService(
        NMaster::IBootstrap* bootstrap,
        TObjectServiceConfigPtr config,
        const NRpc::TServiceDescriptor& descriptor)
        : NMaster::TServiceBase(
            bootstrap,
            descriptor,
            NApi::Logger(),
            bootstrap->GetAuthenticationManager()->GetRpcAuthenticator())
        , RequestHandler_(bootstrap, config->EnforceReadPermissions)
        , Config_(std::move(config))
    {
        auto configDefaults = New<NRpc::TServiceCommonConfig>();

        // Configures sensor types for methods registered below.
        // Configuration is here because it only affects methods registered after it.
        DoConfigureHistogramTimer(configDefaults, Config_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(GenerateTimestamp)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CommitTransaction)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AbortTransaction)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateObject)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateObjects)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveObject)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RemoveObjects)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateObject)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UpdateObjects)
            .SetCancelable(Config_->EnableMutatingRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetObject)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetObjects)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectObjects)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AggregateObjects)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WatchObjects)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckObjectPermissions)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetObjectAccessAllowedFor)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetUserAccessAllowedTo)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SelectObjectHistory)
            .SetCancelable(Config_->EnableRequestCancelation)
            .SetInvokerProvider(BIND(&TObjectService::GetExecutionInvoker, Unretained(this))));

        // Object service is shared between several servers, so the defaults are not provided.
        DoConfigure(std::move(configDefaults), Config_);
    }

private:
    const TRequestHandler RequestHandler_;
    const TObjectServiceConfigPtr Config_;

    TObjectServiceSensorsHolder SensorHolder_;

    IInvokerPtr GetExecutionInvoker(const NRpc::NProto::TRequestHeader& requestHeader)
    {
        if (requestHeader.has_user()) {
            auto pool = SelectExecutionPoolTag(
                Bootstrap_->GetAccessControlManager(),
                requestHeader.user(),
                /*userTagInsteadOfPool*/ true);
            if (pool) {
                auto tag = requestHeader.has_user_tag() ? requestHeader.user_tag() : "";
                return Bootstrap_->GetWorkerPoolInvoker(*pool, tag);
            }
            YT_LOG_WARNING("User pool is empty, fallback on default ("
                "RequestId: %v, "
                "User: %v)",
                requestHeader.has_request_id() ? requestHeader.request_id() : NYT::NProto::TGuid(),
                requestHeader.user());
        }
        return Bootstrap_->GetWorkerPoolInvoker("default", "");
    }

    std::optional<typename TObjectServiceProtoModule::EEventType> TryConvertEventTypeNameToValue(TStringBuf literal)
    {
        static const auto* Type = NYson::ReflectProtobufEnumType(
            TObjectServiceProtoModule::EEventType_descriptor());
        return FindProtobufEnumValueByLiteral<typename TObjectServiceProtoModule::EEventType>(
            Type,
            literal);
    }

    void ValidateEventTypeValue(TEventTypeValue value)
    {
        static const auto* Type = NYson::ReflectProtobufEnumType(
            TObjectServiceProtoModule::EEventType_descriptor());
        YT_VERIFY(FindProtobufEnumLiteralByValue(Type, value));
    }

    TObjectTypeValue PrepareObjectType(typename TObjectServiceProtoModule::EObjectType protoType)
    {
        return NClient::NObjects::GetGlobalObjectTypeRegistry()
            ->GetTypeByValueOrThrow(TObjectTypeRemapper::Remap(protoType))->Value;
    }

    template <class TContextPtr>
    void LogDeprecatedPayloadFormat(const TContextPtr& context)
    {
        YT_LOG_DEBUG("Deprecated payload format (RequestId: %v, %v)",
            context->GetRequestId(),
            context->GetAuthenticationIdentity());
    }

    NYTree::INodePtr PayloadToNode(
        const NClient::NProto::TPayload& payload,
        TObjectTypeValue type,
        const NYPath::TYPath& path)
    {
        if (payload.has_yson()) {
            return payload.yson() ? NYTree::ConvertToNode(NYson::TYsonString(payload.yson())) : nullptr;
        } else if (payload.has_protobuf()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto* typeHandler = objectManager->GetTypeHandlerOrCrash(type);
            const auto* rootType = typeHandler->GetRootProtobufType();
            return NAttributes::ConvertProtobufToNode(rootType, path, payload.protobuf());
        } else {
            return nullptr;
        }
    }

    template <class TContextPtr>
    TUpdateRequest ParseSetUpdate(
        const TContextPtr& context,
        TObjectTypeValue type,
        const typename TObjectServiceProtoModule::TSetUpdate& protoUpdate,
        bool* deprecatedPayloadFormatLogged, // Null means old payload is banned.
        bool valueRequired)
    {
        const auto& path = protoUpdate.path();
        NYTree::INodePtr value;
        if (protoUpdate.has_value()) {
            if (deprecatedPayloadFormatLogged == nullptr) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Use |value_payload|; |value| is not supported");
            }
            value = NYTree::ConvertToNode(NYson::TYsonString(protoUpdate.value()));
            if (!*deprecatedPayloadFormatLogged) {
                LogDeprecatedPayloadFormat(context);
                *deprecatedPayloadFormatLogged = true;
            }
        } else if (protoUpdate.has_value_payload()) {
            value = PayloadToNode(protoUpdate.value_payload(), type, path);
            if (!value) {
                value = NYTree::GetEphemeralNodeFactory()->CreateEntity();
            }
        } else if (!valueRequired) {
            value = NYTree::GetEphemeralNodeFactory()->CreateEntity();
        } else {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Neither |value| nor |value_payload| is given");
        }
        return TSetUpdateRequest{
            path,
            value,
            protoUpdate.recursive(),
            YT_PROTO_OPTIONAL(protoUpdate, shared_write),
            CheckedEnumCast<NClient::NNative::EAggregateMode>(protoUpdate.aggregate_mode())};
    }

    void ParseSetRootUpdateTo(
        TObjectTypeValue type,
        const typename TObjectServiceProtoModule::TSetRootUpdate& protoUpdate,
        std::vector<TUpdateRequest>& updates,
        bool valueRequired)
    {
        NYTree::INodePtr root;
        if (protoUpdate.has_value_payload()) {
            root = PayloadToNode(protoUpdate.value_payload(), type, NYPath::TYPath());
        } else if (valueRequired) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "The field |value_payload| must be given");
        }
        if (!root) {
            root = NYTree::GetEphemeralNodeFactory()->CreateEntity();
        }
        for (const auto& path : protoUpdate.paths()) {
            updates.push_back(TSetUpdateRequest{
                path,
                NAttributes::GetNodeByPathOrEntity(root, path),
                protoUpdate.recursive(),
                YT_PROTO_OPTIONAL(protoUpdate, shared_write),
                CheckedEnumCast<NClient::NNative::EAggregateMode>(protoUpdate.aggregate_mode())});
        }
    }

    TLockUpdateRequest ParseLockUpdate(const typename TObjectServiceProtoModule::TLockUpdate& protoUpdate)
    {
        return TLockUpdateRequest{
            .Path = protoUpdate.path(),
            .LockType = CheckedEnumCast<NTableClient::ELockType>(protoUpdate.lock_type()),
        };
    }

    TUpdateRequest ParseMethodCall(
        TObjectTypeValue type,
        const typename TObjectServiceProtoModule::TMethodCall& protoUpdate,
        bool valueRequired)
    {
        const auto& path = protoUpdate.path();
        NYTree::INodePtr value;
        if (protoUpdate.has_value_payload()) {
            value = PayloadToNode(protoUpdate.value_payload(), type, path);
            if (!value) {
                value = NYTree::GetEphemeralNodeFactory()->CreateEntity();
            }
        } else if (!valueRequired) {
            value = NYTree::GetEphemeralNodeFactory()->CreateEntity();
        } else {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "The field |value_payload| must be given");
        }
        return TMethodRequest{path, std::move(value)};
    }

    template <class TContextPtr, class TRequestPtr>
    NYTree::IMapNodePtr ParseObjectAttributes(
        const TContextPtr& context,
        const TRequestPtr& request,
        TObjectTypeValue objectType,
        bool* deprecatedPayloadFormatLogged)
    {
        NYTree::INodePtr attributes;
        if (request->has_attributes()) {
            if (!*deprecatedPayloadFormatLogged) {
                LogDeprecatedPayloadFormat(context);
                *deprecatedPayloadFormatLogged = true;
            }
            attributes = NYTree::ConvertTo<NYTree::IMapNodePtr>(NYson::TYsonString(request->attributes()));
        } else if (request->has_attributes_payload()) {
            attributes = PayloadToNode(
                request->attributes_payload(),
                objectType,
                NYPath::TYPath());
        }

        if (!attributes) {
            attributes = NYTree::GetEphemeralNodeFactory()->CreateMap();
        }
        return attributes->AsMap();
    }

    NClient::NProto::TPayload ObjectIdentityMetaToPayload(
        NYson::TYsonString yson,
        TObjectTypeValue objectType,
        NClient::NProto::EPayloadFormat format)
    {
        return YsonStringToPayload<TObjectServiceProtoModule>(
            yson,
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrCrash(objectType)->GetRootProtobufType(),
            "/meta",
            Config_->ProtobufFormatUnknownFieldsMode,
            format);
    }

    TKeyAttributeMatches ParseFromIdOrMeta(
        TObjectTypeValue objectType,
        const TString& legacyObjectId,
        const NClient::NProto::TPayload& meta)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto* typeHandler = objectManager->GetTypeHandlerOrCrash(objectType);
        const auto& keyFields = typeHandler->GetKeyFields();

        TKeyAttributeMatches result;
        if (legacyObjectId) {
            if (meta.has_yson() || meta.has_protobuf()) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Object id %Qv and meta cannot be both specified",
                    legacyObjectId);
            }
            // TODO(deep): prohibit polymorphic keys in object_id after switching the python client
            // to meta/key after compacting tests for v70 and 71 migrations.
            result.Key = ParseObjectKey(legacyObjectId, keyFields);
            return result;
        }

        auto node = PayloadToNode(meta, objectType, "/meta");
        if (node) {
            result = MatchKeyAttributes(typeHandler, node);
        }
        return result;
    }

    static TTimestampOrTransactionId GetTimestampOrTransactionId(TTimestamp timestamp, TTransactionId transactionId)
    {
        if (timestamp && transactionId) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                "Parameter timestamp and timestamp_by_transaction_id cannot be both specified");
        }
        // The NullTimestamp is default value.
        return transactionId ? TTimestampOrTransactionId(transactionId) : TTimestampOrTransactionId(timestamp);
    }

    static TTimestampOrTransactionId GetTimestampOrTransactionId(
        bool* readUncommittedChanges,
        TTimestamp timestamp,
        TTransactionId timestampByTransactionId,
        TTransactionId transactionId)
    {
        *readUncommittedChanges = false;

        int optionsEncountered = 0;
        TTimestampOrTransactionId result = NTransactionClient::NullTimestamp;

        if (timestamp) {
            result = timestamp;
            ++optionsEncountered;
        }

        if (timestampByTransactionId) {
            result = timestampByTransactionId;
            ++optionsEncountered;
        }

        if (transactionId) {
            result = transactionId;
            *readUncommittedChanges = true;
            ++optionsEncountered;
        }

        THROW_ERROR_EXCEPTION_IF(optionsEncountered > 1,
            "Only one of `timestamp`, `timestamp_by_transaction_id`, `transaction_id` options could be specified");
        return result;
    }

    static auto GetTimestampOrTransactionIdFormatter(const TTimestampOrTransactionId& timestampOrTransactionId)
    {
        return MakeFormatterWrapper([=] (auto* builder) {
            Visit(timestampOrTransactionId,
                [&] (TTransactionId transactionId) {
                    builder->AppendFormat("TransactionId: %v", transactionId);
                },
                [&] (TTimestamp timestamp) {
                    builder->AppendFormat("Timestamp: %v", timestamp);
                });
        });
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqGenerateTimestamp,
        typename TObjectServiceProtoModule::TRspGenerateTimestamp,
        GenerateTimestamp)
    {
        Y_UNUSED(request);
        context->SetRequestInfo();

        const auto timestamp = RequestHandler_.GenerateTimestamps(context);

        response->set_timestamp(timestamp);
        context->SetResponseInfo("Timestamp: %v", timestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqStartTransaction,
        typename TObjectServiceProtoModule::TRspStartTransaction,
        StartTransaction)
    {
        TStartReadWriteTransactionOptions options;
        if (request->has_start_timestamp()) {
            options.StartTimestamp = request->start_timestamp();
        }
        if (request->has_underlying_transaction_id()) {
            FromProto(&options.UnderlyingTransactionId, request->underlying_transaction_id());
        }
        if (request->has_underlying_transaction_address()) {
            FromProto(&options.UnderlyingTransactionAddress, request->underlying_transaction_address());
        }
        context->SetRequestInfo(
            "StartTimestamp: %v, "
            "UnderlyingTransactionId: %v, "
            "UnderlyingTransactionAddress: %v",
            options.StartTimestamp,
            options.UnderlyingTransactionId,
            options.UnderlyingTransactionAddress);

        if (request->common_options().fetch_performance_statistics()) {
            THROW_ERROR_EXCEPTION("Unsupported option `fetch_performance_statistics` for StartTransaction request");
        }

        if (request->common_options().has_allow_full_scan()) {
            options.ReadingTransactionOptions.AllowFullScan.emplace(
                request->common_options().allow_full_scan());
        }
        options.MutatingTransactionOptions = FromProto<TMutatingTransactionOptions>(
            request->mutating_transaction_options());

        const auto result = RequestHandler_.StartTransaction(context, std::move(options));

        ToProto(response->mutable_transaction_id(), result.TransactionId);
        response->set_start_timestamp(result.StartTimestamp);
        *response->mutable_start_time() = NProtoInterop::CastToProto(result.StartTime);

        context->SetResponseInfo(
            "TransactionId: %v, "
            "StartTimestamp: %v, "
            "StartTime: %v",
            response->transaction_id(),
            response->start_timestamp(),
            result.StartTime);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqCommitTransaction,
        typename TObjectServiceProtoModule::TRspCommitTransaction,
        CommitTransaction)
    {
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo("TransactionId: %v", transactionId);
        context->SetIncrementalResponseInfo("TransactionId: %v", transactionId);

        auto options = TCommitTransactionOptions{
            .TransactionContext = FromProto<TTransactionContext>(request->transaction_context()),
            .CollectStatistics = request->common_options().fetch_performance_statistics(),
        };

        const auto result = RequestHandler_.CommitTransaction(
            context,
            transactionId,
            std::move(options));

        FillPerformanceStatistics(response->mutable_performance_statistics(), result.PerformanceStatistics);

        response->set_commit_timestamp(result.CommitResult.CommitTimestamp);

        *response->mutable_start_time() = NProtoInterop::CastToProto(result.CommitResult.StartTime);
        *response->mutable_finish_time() = NProtoInterop::CastToProto(result.CommitResult.FinishTime);
        context->SetResponseInfo(
            "CommitTimestamp: %v, "
            "StartTime: %v, "
            "FinishTime: %v",
            result.CommitResult.CommitTimestamp,
            result.CommitResult.StartTime,
            result.CommitResult.FinishTime);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqAbortTransaction,
        typename TObjectServiceProtoModule::TRspAbortTransaction,
        AbortTransaction)
    {
        Y_UNUSED(response);
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        context->SetRequestInfo("TransactionId: %v", transactionId);

        RequestHandler_.AbortTransaction(context, transactionId);

        context->Reply();
    }

    template <class TContextPtr>
    TUpdateIfExistingRequest ParseUpdateIfExisting(
        const TContextPtr& context,
        TObjectTypeValue objectType,
        const typename TObjectServiceProtoModule::TUpdateIfExisting& request,
        IAttributeValuesConsumer* consumer)
    {
        TUpdateIfExistingRequest result;
        result.Requests = ParseUpdates(
            context,
            objectType,
            request,
            /*deprecatedPayloadFormatLogged*/ nullptr,
            /*valueRequired*/ false);
        result.Prerequisites = FromProto<std::vector<TAttributeTimestampPrerequisite>>(
            request.attribute_timestamp_prerequisites());
        result.Consumer = consumer;

        return result;
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqCreateObject,
        typename TObjectServiceProtoModule::TRspCreateObject,
        CreateObject)
    {
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto objectType = PrepareObjectType(request->object_type());

        TCreateObjectSubrequest createSubrequest;
        bool deprecatedPayloadFormatLogged = false;
        const auto format = request->format();
        const bool needObjectIdentityMeta = format != NClient::NProto::EPayloadFormat::PF_NONE;

        createSubrequest.Type = PrepareObjectType(request->object_type());
        auto commonOptions = GetCommonOptions(request->common_options());

        // Log request info as early as practical.
        context->SetRequestInfo("TransactionId: %v, ObjectType: %v, CommonOptions: %v",
            transactionId,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(createSubrequest.Type),
            commonOptions);

        auto* typeHandler = Bootstrap_->GetObjectManager()
            ->GetTypeHandlerOrThrow(createSubrequest.Type);

        THROW_ERROR_EXCEPTION_IF(!needObjectIdentityMeta && HasCompositeOrPolymorphicKey(typeHandler),
            NClient::EErrorCode::InvalidRequestArguments,
            "Specify a format for meta response when creating an object of type %Qv; "
            "only single string ids are returned in object_id",
            NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeNameByValueOrCrash(typeHandler->GetType()));

        createSubrequest.Attributes = ParseObjectAttributes(
            context,
            request,
            createSubrequest.Type,
            &deprecatedPayloadFormatLogged);

        std::span container(response, 1);
        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            container,
            request->format(),
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(objectType)->GetRootProtobufType(),
            Config_->ProtobufFormatUnknownFieldsMode);
        auto consumer = consumerGroup->CreateConsumers(1);

        if (request->has_update_if_existing()) {
            createSubrequest.UpdateIfExisting = ParseUpdateIfExisting(
                context,
                createSubrequest.Type,
                request->update_if_existing(),
                consumer[0].get());
        }

        TCreateObjectsRequest nativeRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = {std::move(createSubrequest)},
            .NeedIdentityMeta = needObjectIdentityMeta,
        };

        const auto result = RequestHandler_.CreateObjects(
            context,
            transactionId,
            std::move(nativeRequest));

        if (needObjectIdentityMeta) {
            *response->mutable_meta() =
                ObjectIdentityMetaToPayload(result.ObjectInfos[0].IdentityMeta, result.ObjectInfos[0].Type, format);
        } else {
            response->set_object_id(result.ObjectInfos[0].Key.ToString());
            response->set_fqid(result.ObjectInfos[0].Fqid);
        }

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);

        context->SetResponseInfo("ObjectKey: %v, CommitTimestamp: %v",
            result.ObjectInfos[0].Key,
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqCreateObjects,
        typename TObjectServiceProtoModule::TRspCreateObjects,
        CreateObjects)
    {
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        std::vector<TCreateObjectSubrequest> createSubrequests;
        bool deprecatedPayloadFormatLogged = false;
        createSubrequests.reserve(request->subrequests_size());

        const auto format = request->format();
        const bool needObjectIdentityMeta = format != NClient::NProto::EPayloadFormat::PF_NONE;

        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            *response->mutable_subresponses(),
            request->format(),
            nullptr,
            Config_->ProtobufFormatUnknownFieldsMode);
        auto consumers = consumerGroup->CreateConsumers(request->subrequests().size());

        for (const auto& [protoSubrequest, consumer] : Zip(request->subrequests(), consumers)) {
            createSubrequests.push_back({});
            auto& createSubrequest = createSubrequests.back();
            createSubrequest.Type = PrepareObjectType(protoSubrequest.object_type());
            auto* typeHandler = Bootstrap_->GetObjectManager()
                ->GetTypeHandlerOrThrow(createSubrequest.Type);
            THROW_ERROR_EXCEPTION_IF(!needObjectIdentityMeta && HasCompositeOrPolymorphicKey(typeHandler),
                NClient::EErrorCode::InvalidRequestArguments,
                "Specify a format for meta response when creating an object of type %Qv; "
                "only single string ids are returned in object_id",
                NClient::NObjects::GetGlobalObjectTypeRegistry()
                    ->GetTypeNameByValueOrCrash(typeHandler->GetType()));

            createSubrequest.Attributes = ParseObjectAttributes(
                context,
                &protoSubrequest,
                createSubrequest.Type,
                &deprecatedPayloadFormatLogged);
            if (protoSubrequest.has_update_if_existing()) {
                createSubrequest.UpdateIfExisting = ParseUpdateIfExisting(
                    context,
                    createSubrequest.Type,
                    protoSubrequest.update_if_existing(),
                    consumer.get());
            }
        }
        auto commonOptions = GetCommonOptions(request->common_options());

        // Log request info as early as practical.
        context->SetRequestInfo("TransactionId: %v, Subrequests: %v, CommonOptions: %v",
            transactionId,
            MakeShrunkFormattableView(
                createSubrequests,
                [] (auto* builder, const auto& createSubrequest) {
                    builder->AppendFormat("{ObjectType: %v}", NClient::NObjects::GetGlobalObjectTypeRegistry()
                        ->GetTypeNameByValueOrCrash(createSubrequest.Type));
                },
                Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
            commonOptions);

        TCreateObjectsRequest nativeRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = std::move(createSubrequests),
            .NeedIdentityMeta = needObjectIdentityMeta,
        };
        const auto result = RequestHandler_.CreateObjects(
            context,
            transactionId,
            std::move(nativeRequest));

        for (const auto& [index, objectInfo] : Enumerate(result.ObjectInfos)) {
            auto* subresponse = response->mutable_subresponses(index);
            if (needObjectIdentityMeta) {
                *subresponse->mutable_meta() = ObjectIdentityMetaToPayload(
                    objectInfo.IdentityMeta,
                    objectInfo.Type,
                    format);
            } else {
                subresponse->set_object_id(objectInfo.Key.ToString());
                subresponse->set_fqid(objectInfo.Fqid);
            }
        }

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);

        context->SetResponseInfo("ObjectKeys: %v, CommitTimestamp: %v",
            MakeShrunkFormattableView(
                result.ObjectInfos,
                [] (auto* builder, const auto& objectInfo) {
                    builder->AppendString(objectInfo.Key.ToString());
                },
                Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqRemoveObject,
        typename TObjectServiceProtoModule::TRspRemoveObject,
        RemoveObject)
    {
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto objectType = PrepareObjectType(request->object_type());
        auto match = ParseFromIdOrMeta(
            objectType,
            request->object_id(),
            request->meta());
        auto commonOptions = GetCommonOptions(request->common_options());

        context->SetRequestInfo("TransactionId: %v, ObjectType: %v, ObjectKey: %v, CommonOptions: %v",
            transactionId,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            match.Key,
            commonOptions);

        TRemoveObjectsRequest nativeRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = {TRemoveObjectsRequest::TSubrequest{
                .Type = objectType,
                .Match = std::move(match)
            }},
            .IgnoreNonexistent = request->ignore_nonexistent(),
        };
        if (request->has_allow_removal_with_non_empty_reference()) {
            nativeRequest.AllowRemovalWithNonEmptyReferences.emplace(
                request->allow_removal_with_non_empty_reference());
        }
        const TRemoveObjectsResponse result = RequestHandler_.RemoveObjects(
            context,
            transactionId,
            std::move(nativeRequest));

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);
        YT_VERIFY(std::ssize(result.Subresponses) == 1);
        response->set_finalization_start_time(result.Subresponses[0].FinalizationStartTime.GetValue());

        context->SetResponseInfo("CommitTimestamp: %v",
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqRemoveObjects,
        typename TObjectServiceProtoModule::TRspRemoveObjects,
        RemoveObjects)
    {
        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        std::vector<TRemoveObjectsRequest::TSubrequest> subrequests;
        subrequests.reserve(request->subrequests_size());
        for (const auto& subrequest : request->subrequests()) {
            const auto objectType = PrepareObjectType(subrequest.object_type());
            auto match = ParseFromIdOrMeta(
                objectType,
                subrequest.object_id(),
                subrequest.meta());
            subrequests.push_back({objectType, std::move(match)});
        }
        auto commonOptions = GetCommonOptions(request->common_options());

        context->SetRequestInfo("TransactionId: %v, Subrequests: %v, CommonOptions: %v",
            transactionId,
            MakeShrunkFormattableView(
                subrequests,
                [] (auto* builder, const auto& subrequest) {
                    builder->AppendFormat("{ObjectType: %v, ObjectKey: %v}",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()
                            ->GetTypeNameByValueOrCrash(subrequest.Type),
                        subrequest.Match.Key);
                },
                Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
            commonOptions);

        TRemoveObjectsRequest removeObjectsRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = std::move(subrequests),
            .IgnoreNonexistent = request->ignore_nonexistent(),
        };
        if (request->has_allow_removal_with_non_empty_reference()) {
            removeObjectsRequest.AllowRemovalWithNonEmptyReferences.emplace(
                request->allow_removal_with_non_empty_reference());
        }
        const TRemoveObjectsResponse result = RequestHandler_.RemoveObjects(
            context,
            transactionId,
            std::move(removeObjectsRequest));

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);
        for (int index = 0; index < std::ssize(result.Subresponses); ++index) {
            auto* subresponse = response->add_subresponses();
            subresponse->set_finalization_start_time(result.Subresponses[index].FinalizationStartTime.GetValue());
        }

        context->SetResponseInfo("CommitTimestamp: %v",
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    template <class TContextPtr, class TProtoUpdates>
    std::vector<TUpdateRequest> ParseUpdates(
        const TContextPtr& context,
        TObjectTypeValue objectType,
        const TProtoUpdates& protoUpdates,
        bool* deprecatedPayloadFormatLogged = nullptr,
        bool valueRequired = true)
    {
        auto updateCount = protoUpdates.set_updates().size() +
            protoUpdates.remove_updates().size() +
            protoUpdates.lock_updates().size() +
            protoUpdates.method_calls().size();
        for (const auto& update : protoUpdates.set_root_updates()) {
            updateCount += update.paths().size();
        }

        std::vector<TUpdateRequest> updates;
        updates.reserve(updateCount);
        for (const auto& update : protoUpdates.set_updates()) {
            updates.push_back(ParseSetUpdate(
                context,
                objectType,
                update,
                deprecatedPayloadFormatLogged,
                valueRequired));
        }
        for (const auto& update : protoUpdates.set_root_updates()) {
            ParseSetRootUpdateTo(objectType, update, updates, valueRequired);
        }
        for (const auto& update : protoUpdates.remove_updates()) {
            updates.push_back(FromProto<TRemoveUpdateRequest>(update));
        }
        for (const auto& update : protoUpdates.lock_updates()) {
            updates.push_back(ParseLockUpdate(update));
        }
        for (const auto& method : protoUpdates.method_calls()) {
            updates.push_back(ParseMethodCall(objectType, method, valueRequired));
        }
        return updates;
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqUpdateObject,
        typename TObjectServiceProtoModule::TRspUpdateObject,
        UpdateObject)
    {
        Y_UNUSED(response);

        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto objectType = PrepareObjectType(request->object_type());
        auto match = ParseFromIdOrMeta(
            objectType,
            request->object_id(),
            request->meta());

        bool deprecatedPayloadFormatLogged = false;
        std::span container(response, 1);
        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            container,
            request->format(),
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(objectType)->GetRootProtobufType(),
            Config_->ProtobufFormatUnknownFieldsMode);
        auto consumer = consumerGroup->CreateConsumers(1);
        auto updates = ParseUpdates(
            context,
            objectType,
            *request,
            &deprecatedPayloadFormatLogged);
        auto prerequisites = FromProto<std::vector<TAttributeTimestampPrerequisite>>(
            request->attribute_timestamp_prerequisites());
        auto commonOptions = GetCommonOptions(request->common_options());

        context->SetRequestInfo(
            "TransactionId: %v, "
            "ObjectType: %v, "
            "ObjectKey: %v, "
            "UpdateCount: %v, "
            "PrerequisiteCount: %v, "
            "CommonOptions: %v",
            transactionId,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            match.Key,
            updates.size(),
            prerequisites.size(),
            commonOptions);

        TUpdateObjectsRequest nativeRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = {
                TUpdateObjectsRequest::TSubrequest{
                    .Type = objectType,
                    .Match = std::move(match),
                    .Updates = std::move(updates),
                    .Prerequisites = std::move(prerequisites),
                    .Consumer = consumer[0].get()
                },
            },
            .IgnoreNonexistent = request->ignore_nonexistent(),
        };
        const auto result = RequestHandler_.UpdateObjects(
            context,
            transactionId,
            std::move(nativeRequest));

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);

        context->SetResponseInfo("CommitTimestamp: %v",
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqUpdateObjects,
        typename TObjectServiceProtoModule::TRspUpdateObjects,
        UpdateObjects)
    {
        Y_UNUSED(response);

        const auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            *response->mutable_subresponses(),
            request->format(),
            nullptr,
            Config_->ProtobufFormatUnknownFieldsMode);

        auto consumers = consumerGroup->CreateConsumers(request->subrequests_size());
        auto commonOptions = GetCommonOptions(request->common_options());
        std::vector<TUpdateObjectsRequest::TSubrequest> subrequests;
        subrequests.reserve(request->subrequests_size());
        for (const auto& [subrequest, consumer] : Zip(request->subrequests(), consumers)) {
            const auto objectType = PrepareObjectType(subrequest.object_type());
            auto match = ParseFromIdOrMeta(
                objectType,
                subrequest.object_id(),
                subrequest.meta());
            bool deprecatedPayloadFormatLogged = false;
            auto updates = ParseUpdates(context, objectType, subrequest, &deprecatedPayloadFormatLogged);
            auto prerequisites = FromProto<std::vector<TAttributeTimestampPrerequisite>>(
                subrequest.attribute_timestamp_prerequisites());

            subrequests.push_back({
                objectType,
                std::move(match),
                std::move(updates),
                std::move(prerequisites),
                consumer.get()
            });
        }

        context->SetRequestInfo("TransactionId: %v, Subrequests: %v, CommonOptions: %v",
            transactionId,
            MakeShrunkFormattableView(
                subrequests,
                [] (auto* builder, const auto& subrequest) {
                    builder->AppendFormat(
                        "{ObjectType: %v, ObjectKey: %v, UpdateCount: %v, PrerequisiteCount: %v}",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()
                            ->GetTypeNameByValueOrCrash(subrequest.Type),
                        subrequest.Match.Key,
                        subrequest.Updates.size(),
                        subrequest.Prerequisites.size());
                },
                Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
            commonOptions);

        TUpdateObjectsRequest nativeRequest{
            .CommonOptions = std::move(commonOptions),
            .Subrequests = std::move(subrequests),
            .IgnoreNonexistent = request->ignore_nonexistent(),
        };
        const auto result = RequestHandler_.UpdateObjects(
            context,
            transactionId,
            std::move(nativeRequest));

        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonWriteResult.PerformanceStatistics);
        FillCommitTimestamp(response, result.CommonWriteResult.CommitTimestamp);

        context->SetResponseInfo("CommitTimestamp: %v",
            result.CommonWriteResult.CommitTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqGetObject,
        typename TObjectServiceProtoModule::TRspGetObject,
        GetObject)
    {
        const auto objectType = PrepareObjectType(request->object_type());
        std::vector<TKeyAttributeMatches> matches;
        matches.push_back(ParseFromIdOrMeta(objectType, request->object_id(), request->meta()));
        const auto& match = matches.back();
        TAttributeSelector selector{
            FromProto<std::vector<TString>>(request->selector().paths())
        };
        auto queryOptions = FromProto<TGetQueryOptions>(request->options());
        const auto format = request->format();
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(
            &queryOptions.ReadUncommittedChanges,
            request->timestamp(),
            FromProto<TTransactionId>(request->timestamp_by_transaction_id()),
            FromProto<TTransactionId>(request->transaction_id()));
        auto commonOptions = GetCommonOptions(request->common_options());
        context->SetRequestInfo(
            "Attributes: %v, ObjectType: %v, %v, Selector: %v, Options: %v, Format: %v, CommonOptions: %v",
            match,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            selector.Paths,
            queryOptions,
            CheckedEnumCast<NClient::NNative::EPayloadFormat>(format),
            commonOptions);

        if (format == NClient::NProto::EPayloadFormat::PF_NONE) {
            LogDeprecatedPayloadFormat(context);
        }

        TGetObjectsRequest getObjectRequest{
            .CommonOptions = std::move(commonOptions),
            .Selector = std::move(selector),
            .QueryOptions = std::move(queryOptions),
            .ObjectType = objectType,
            .TimestampOrTransactionId = timestampOrTransactionId,
        };

        std::span container(response, 1);
        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            container,
            format,
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(objectType)->GetRootProtobufType(),
            Config_->ProtobufFormatUnknownFieldsMode);

        auto result = RequestHandler_.GetObjects(
            context,
            matches,
            consumerGroup.get(),
            std::move(getObjectRequest));

        response->set_timestamp(result.CommonReadResult.StartTimestamp);
        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonReadResult.PerformanceStatistics);

        context->SetResponseInfo("Timestamp: %v", result.CommonReadResult.StartTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqGetObjects,
        typename TObjectServiceProtoModule::TRspGetObjects,
        GetObjects)
    {
        const auto objectType = PrepareObjectType(request->object_type());
        TAttributeSelector selector{
            FromProto<std::vector<TString>>(request->selector().paths())
        };

        auto queryOptions = FromProto<TGetQueryOptions>(request->options());

        std::vector<TKeyAttributeMatches> matches;
        matches.reserve(request->subrequests().size());
        for (const auto& subrequest : request->subrequests()) {
            matches.push_back(ParseFromIdOrMeta(
                objectType,
                subrequest.object_id(),
                subrequest.meta()));
        }

        const auto format = request->format();
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(
            &queryOptions.ReadUncommittedChanges,
            request->timestamp(),
            FromProto<TTransactionId>(request->timestamp_by_transaction_id()),
            FromProto<TTransactionId>(request->transaction_id()));
        auto commonOptions = GetCommonOptions(request->common_options());

        context->SetRequestInfo(
            "Attributes: %v, ObjectType: %v, %v, Selector: %v, Options: %v, Format: %v, CommonOptions: %v",
            MakeShrunkFormattableView(
                matches,
                TDefaultFormatter(),
                Bootstrap_->GetInitialConfig()->LogCountLimitForPluralRequests),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            selector.Paths,
            queryOptions,
            CheckedEnumCast<NClient::NNative::EPayloadFormat>(format),
            commonOptions);

        if (format == NClient::NProto::EPayloadFormat::PF_NONE) {
            LogDeprecatedPayloadFormat(context);
        }

        TGetObjectsRequest getObjectRequest{
            .CommonOptions = std::move(commonOptions),
            .Selector = std::move(selector),
            .QueryOptions = std::move(queryOptions),
            .ObjectType = objectType,
            .TimestampOrTransactionId = timestampOrTransactionId,
        };

        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            *response->mutable_subresponses(),
            format,
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(objectType)->GetRootProtobufType(),
            Config_->ProtobufFormatUnknownFieldsMode);

        const auto result = RequestHandler_.GetObjects(
            context,
            matches,
            consumerGroup.get(),
            std::move(getObjectRequest));

        response->set_timestamp(result.CommonReadResult.StartTimestamp);
        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonReadResult.PerformanceStatistics);

        context->SetResponseInfo("Timestamp: %v", result.CommonReadResult.StartTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqWatchObjects,
        typename TObjectServiceProtoModule::TRspWatchObjects,
        WatchObjects)
    {
        const auto objectType = PrepareObjectType(request->object_type());

        TWatchQueryOptions queryOptions;
        queryOptions.ObjectType = objectType;
        FromProto(&queryOptions, request);

        auto commonOptions = GetCommonOptions(request->common_options());
        context->SetRequestInfo("ObjectType: %v, Options: %v, CommonOptions: %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            queryOptions,
            commonOptions);

        TWatchObjectsRequest watchObjectsRequest{
            .CommonOptions = std::move(commonOptions),
            .QueryOptions = std::move(queryOptions),
        };
        const auto result = RequestHandler_.WatchObjects(
            context,
            std::move(watchObjectsRequest));

        response->set_timestamp(result.QueryResult.Timestamp);
        if (request->skip_trimmed()) {
            response->set_skipped_row_count(result.QueryResult.SkippedRowCount);
        } else {
            YT_VERIFY(result.QueryResult.SkippedRowCount == 0);
        }

        const auto format = request->format();
        response->mutable_events()->Reserve(result.QueryResult.Events.size());
        for (const auto& event : result.QueryResult.Events) {
            auto* protoEvent = response->add_events();
            protoEvent->set_timestamp(event.Timestamp);
            if (format == NClient::NProto::EPayloadFormat::PF_NONE) {
                protoEvent->set_object_id(event.ObjectId);
            } else {
                *protoEvent->mutable_meta() =
                    ObjectIdentityMetaToPayload(event.Meta, objectType, format);
            }
            auto optionalEventTypeValue = TryConvertEventTypeNameToValue(event.TypeName);
            YT_VERIFY(optionalEventTypeValue);
            protoEvent->set_event_type(*optionalEventTypeValue);
            if (!event.TransactionContext.Items.empty()) {
                ToProto(
                    protoEvent->mutable_transaction_context(),
                    event.TransactionContext);
            }

            if (request->fetch_changed_attributes() && event.TypeName == ObjectUpdatedEventTypeName) {
                protoEvent->set_changed_attributes_summary(event.ChangedAttributesSummary);
            }

            if (event.TypeName == ObjectUpdatedEventTypeName && !event.ChangedTags.empty()) {
                ToProto(protoEvent->mutable_changed_tags(), event.ChangedTags);
            }

            if (request->fetch_event_index()) {
                ToProto(protoEvent->mutable_index(), event.EventIndex);
            }

            Visit(event.HistoryTime,
                [] (const std::monostate&) {},
                [&] (const TTimestamp& timestamp) {
                    protoEvent->set_history_timestamp(timestamp);
                },
                [&] (const TInstant& time) {
                    *protoEvent->mutable_history_time() = NProtoInterop::CastToProto(time);
                });
        }
        if (request->fetch_changed_attributes()) {
            for (auto& path : result.QueryResult.ChangedAttributes) {
                auto* protoChangedAttribute = response->add_changed_attributes_index();
                protoChangedAttribute->set_attribute_path(std::move(path));
            }
        }

        response->set_continuation_token(SerializeContinuationToken(result.QueryResult.ContinuationToken));

        FillPerformanceStatistics(response->mutable_performance_statistics(), result.PerformanceStatistics);

        context->SetResponseInfo("Count: %v, Timestamp: %v, ContinuationToken: %v",
            response->events_size(),
            response->timestamp(),
            response->continuation_token());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqSelectObjects,
        typename TObjectServiceProtoModule::TRspSelectObjects,
        SelectObjects)
    {
        const auto objectType = PrepareObjectType(request->object_type());

        const auto filter = request->has_filter()
            ? std::make_optional(TObjectFilter{request->filter().query()})
            : std::nullopt;

        TAttributeSelector selector{
            FromProto<std::vector<TString>>(request->selector().paths())
        };

        TObjectOrderBy orderBy;
        FromProto(&orderBy, request->order_by());

        TSelectQueryOptions queryOptions;
        FromProto<
            typename TObjectServiceProtoModule::TSelectObjectsOptions,
            typename TObjectServiceProtoModule::TSelectObjectsContinuationToken>(
            &queryOptions,
            request->options());

        // COMPAT(babenko)
        if (request->has_offset()) {
            queryOptions.Offset = request->offset().value();
        }
        if (request->has_limit()) {
            queryOptions.Limit = request->limit().value();
        }

        auto index = request->has_index()
            ? std::make_optional(TIndex{request->index().name()})
            : std::nullopt;

        const auto format = request->format();
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(
            request->timestamp(),
            FromProto<TTransactionId>(request->timestamp_by_transaction_id()));
        auto commonOptions = GetCommonOptions(request->common_options());
        context->SetRequestInfo(
            "ObjectType: %v, "
            "%v, "
            "Filter: %v, "
            "Selector: %v, "
            "OrderBy: %v, "
            "Options: %v, "
            "Format: %v, "
            "Index: %v, "
            "CommonOptions: %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            filter,
            selector,
            orderBy,
            queryOptions,
            CheckedEnumCast<NClient::NNative::EPayloadFormat>(format),
            index,
            commonOptions);

        if (format == NClient::NProto::EPayloadFormat::PF_NONE) {
            LogDeprecatedPayloadFormat(context);
        }

        TSelectObjectsRequest selectObjectsRequest{
            .CommonOptions = std::move(commonOptions),
            .Selector = std::move(selector),
            .QueryOptions = std::move(queryOptions),
            .Filter = std::move(filter),
            .OrderBy = std::move(orderBy),
            .Index = std::move(index),
            .ObjectType = objectType,
            .TimestampOrTransactionId = timestampOrTransactionId,
        };

        auto consumerGroup = MakeProtoConsumerGroup<TObjectServiceProtoModule>(
            *response->mutable_results(),
            format,
            Bootstrap_->GetObjectManager()->GetTypeHandlerOrThrow(objectType)->GetRootProtobufType(),
            Config_->ProtobufFormatUnknownFieldsMode);

        auto result = RequestHandler_.SelectObjects(
            context,
            consumerGroup.get(),
            std::move(selectObjectsRequest));

        response->set_timestamp(result.CommonReadResult.StartTimestamp);
        if (result.QueryResult.ContinuationToken) {
            typename TObjectServiceProtoModule::TSelectObjectsContinuationToken protoContinuationToken;
            ToProto(&protoContinuationToken, *result.QueryResult.ContinuationToken);
            response->set_continuation_token(SerializeContinuationToken(protoContinuationToken));
        }
        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonReadResult.PerformanceStatistics);

        context->SetResponseInfo("Count: %v, Timestamp: %v, ContinuationToken: %v",
            response->results_size(),
            response->timestamp(),
            response->continuation_token());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqAggregateObjects,
        typename TObjectServiceProtoModule::TRspAggregateObjects,
        AggregateObjects)
    {
        const auto objectType = PrepareObjectType(request->object_type());
        auto filter = request->has_filter()
            ? std::make_optional(TObjectFilter{request->filter().query()})
            : std::nullopt;

        TAttributeGroupingExpressions groupByExpressions{
            FromProto<std::vector<TString>>(request->group_by_expressions().expressions())};
        TAttributeAggregateExpressions aggregateExpressions{
            FromProto<std::vector<TString>>(request->aggregate_expressions().expressions())};

        // Only generic yson can be used for aggregate query.
        const auto format = NClient::NProto::EPayloadFormat::PF_YSON;
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(
            request->timestamp(),
            FromProto<TTransactionId>(request->timestamp_by_transaction_id()));
        auto commonOptions = GetCommonOptions(request->common_options());
        std::optional<bool> fetchFinalizingObjects;
        if (request->has_fetch_finalizing_objects()) {
            fetchFinalizingObjects.emplace(request->fetch_finalizing_objects());
        }
        context->SetRequestInfo(
            "ObjectType: %v, %v, Filter: %v, Aggregators: %v, GroupByExpressions: %v, Format: %v, CommonOptions: %v%v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            filter,
            aggregateExpressions,
            groupByExpressions,
            CheckedEnumCast<NClient::NNative::EPayloadFormat>(format),
            commonOptions,
            fetchFinalizingObjects ? Format("FetchFinalizingObjects: %v", *fetchFinalizingObjects) : "");

        TAggregateObjectsRequest aggregateObjectsRequest{
            .CommonOptions = std::move(commonOptions),
            .Filter = std::move(filter),
            .AggregateExpressions = std::move(aggregateExpressions),
            .GroupByExpressions = std::move(groupByExpressions),
            .ObjectType = objectType,
            .TimestampOrTransactionId = timestampOrTransactionId,
            .FetchFinalizingObjects = fetchFinalizingObjects,
        };
        auto result = RequestHandler_.AggregateObjects(
            context,
            std::move(aggregateObjectsRequest));

        response->set_timestamp(result.CommonReadResult.StartTimestamp);
        response->mutable_results()->Reserve(result.QueryResult.Objects.size());
        for (auto& object : result.QueryResult.Objects) {
            auto* protoResult = response->add_results();
            NObjects::MoveObjectResultToProto<TObjectServiceProtoModule>(
                format,
                Bootstrap_->GetObjectManager()->GetTypeHandlerOrCrash(objectType)->GetRootProtobufType(),
                result.Selector,
                &object,
                Config_->ProtobufFormatUnknownFieldsMode,
                protoResult,
                /*fetchRootObject*/ false);
        }
        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonReadResult.PerformanceStatistics);

        context->SetResponseInfo("Count: %v, Timestamp: %v",
            result.QueryResult.Objects.size(),
            result.CommonReadResult.StartTimestamp);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqCheckObjectPermissions,
        typename TObjectServiceProtoModule::TRspCheckObjectPermissions,
        CheckObjectPermissions)
    {
        const auto transactionId = FromProto<TTransactionId>(request->timestamp_by_transaction_id());
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(request->timestamp(), transactionId);

        context->SetRequestInfo("%v, SubrequestCount: %v",
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            request->subrequests_size());

        const auto accessControlManager = Bootstrap_->GetAccessControlManager();
        auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
            context,
            accessControlManager,
            /*requestWeight*/ request->subrequests_size(),
            transactionId);

        auto transaction = TReadOnlyTransactionWrapper(
            Bootstrap_->GetTransactionManager(),
            timestampOrTransactionId,
            context->GetTimeout(),
            AllowFullScanFromOptions(request->common_options()))
            .Unwrap();
        transaction->EnableAccessControlPreload();

        struct TSubrequest
        {
            TKeyAttributeMatches Match;
            TObjectId SubjectId;
            NAccessControl::TAccessControlPermissionValue Permission;
            TString AttributePath;
            TObject* Object;
        };
        std::vector<TSubrequest> subrequests;
        subrequests.reserve(request->subrequests_size());
        for (const auto& subrequest : request->subrequests()) {
            const auto objectType = PrepareObjectType(subrequest.object_type());
            auto match = ParseFromIdOrMeta(
                objectType,
                subrequest.object_id(),
                subrequest.meta());
            auto subjectId = subrequest.subject_id();
            auto permission = accessControlManager->CheckedPermissionValueCast(subrequest.permission());
            auto attributePath = subrequest.attribute_path();
            auto* object = transaction->GetObject(objectType, match.Key, match.ParentKey);
            object->PrepareUuidValidation(match.Uuid);
            object->ScheduleAccessControlLoad();
            subrequests.push_back({
                std::move(match),
                std::move(subjectId),
                permission,
                std::move(attributePath),
                object});
        }

        for (const auto& subrequest : subrequests) {
            subrequest.Object->ValidateUuid(subrequest.Match.Uuid);
            auto result = accessControlManager->CheckPermission(
                subrequest.SubjectId,
                subrequest.Object,
                subrequest.Permission,
                subrequest.AttributePath);
            auto* subresponse = response->add_subresponses();
            subresponse->set_action(
                static_cast<typename TObjectServiceProtoModule::EAccessControlAction>(
                    result.Action));
            subresponse->set_object_id(result.ObjectKey.ToString()); // TODO(deep)
            subresponse->set_object_type(
                static_cast<typename TObjectServiceProtoModule::EObjectType>(result.ObjectType));
            subresponse->set_subject_id(result.SubjectId);
        }

        response->set_timestamp(transaction->GetStartTimestamp());
        MaybeFlushPerformanceStatistics(
            transaction,
            response,
            request->common_options().fetch_performance_statistics());

        context->SetResponseInfo("Timestamp: %v",
            transaction->GetStartTimestamp());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqGetObjectAccessAllowedFor,
        typename TObjectServiceProtoModule::TRspGetObjectAccessAllowedFor,
        GetObjectAccessAllowedFor)
    {
        const auto transactionId = FromProto<TTransactionId>(request->timestamp_by_transaction_id());
        const auto timestampOrTransactionId = GetTimestampOrTransactionId(request->timestamp(), transactionId);
        context->SetRequestInfo(
            "%v, SubrequestCount: %v",
            GetTimestampOrTransactionIdFormatter(timestampOrTransactionId),
            request->subrequests_size());

        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
            context,
            accessControlManager,
            /*requestWeight*/ request->subrequests_size(),
            transactionId);

        auto transaction = TReadOnlyTransactionWrapper(
            Bootstrap_->GetTransactionManager(),
            timestampOrTransactionId,
            context->GetTimeout(),
            AllowFullScanFromOptions(request->common_options()))
            .Unwrap();
        transaction->EnableAccessControlPreload();

        struct TSubrequest
        {
            TKeyAttributeMatches Match;
            NAccessControl::TAccessControlPermissionValue Permission;
            TString AttributePath;
            TObject* Object;
        };
        std::vector<TSubrequest> subrequests;
        subrequests.reserve(request->subrequests_size());
        for (const auto& subrequest : request->subrequests()) {
            const auto objectType = PrepareObjectType(subrequest.object_type());
            auto match = ParseFromIdOrMeta(
                objectType,
                subrequest.object_id(),
                subrequest.meta());
            auto permission = accessControlManager->CheckedPermissionValueCast(subrequest.permission());
            auto attributePath = subrequest.attribute_path();
            auto* object = transaction->GetObject(objectType, match.Key, match.ParentKey);
            object->PrepareUuidValidation(match.Uuid);
            subrequests.push_back({
                    std::move(match),
                    permission,
                    std::move(attributePath),
                    object});
        }

        for (const auto& subrequest : subrequests) {
            subrequest.Object->ValidateUuid(subrequest.Match.Uuid);
            auto userIds = accessControlManager->GetObjectAccessAllowedFor(
                subrequest.Object,
                subrequest.Permission,
                subrequest.AttributePath);
            auto* subresponse = response->add_subresponses();
            ToProto(subresponse->mutable_user_ids(), userIds);
        }

        response->set_timestamp(transaction->GetStartTimestamp());
        MaybeFlushPerformanceStatistics(
            transaction,
            response,
            request->common_options().fetch_performance_statistics());

        context->SetResponseInfo("Timestamp: %v",
            transaction->GetStartTimestamp());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqGetUserAccessAllowedTo,
        typename TObjectServiceProtoModule::TRspGetUserAccessAllowedTo,
        GetUserAccessAllowedTo)
    {
        context->SetRequestInfo("SubrequestCount: %v", request->subrequests_size());

        auto authUserGuard = NAccessControl::MakeAuthenticatedUserGuardAndThrottle(
            context,
            Bootstrap_->GetAccessControlManager(),
            /*requestWeight*/ request->subrequests_size());

        const auto& accessControlManager = Bootstrap_->GetAccessControlManager();
        for (const auto& subrequest : request->subrequests()) {
            const auto objectType = PrepareObjectType(subrequest.object_type());
            const auto permission = accessControlManager->CheckedPermissionValueCast(subrequest.permission());
            const auto& attributePath = subrequest.attribute_path();

            auto filter = subrequest.has_filter()
                ? std::make_optional(TObjectFilter{subrequest.filter().query()})
                : std::nullopt;

            NAccessControl::TGetUserAccessAllowedToOptions options;
            if (subrequest.has_continuation_token()) {
                options.ContinuationToken = subrequest.continuation_token();
            }
            if (subrequest.has_limit()) {
                options.Limit = subrequest.limit();
            }

            auto result = accessControlManager->GetUserAccessAllowedTo(
                subrequest.user_id(),
                objectType,
                permission,
                attributePath,
                filter,
                options);

            auto subresponse = response->add_subresponses();
            for (const auto& key : result.ObjectKeys) {
                ToProto(subresponse->add_object_ids(), key.ToString()); // TODO(deep)
            }
            subresponse->set_continuation_token(std::move(result.ContinuationToken));
        }

        context->SetResponseInfo("SubresponseCount: %v",
            response->subresponses_size());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD_VIA_MESSAGES(
        typename TObjectServiceProtoModule::TReqSelectObjectHistory,
        typename TObjectServiceProtoModule::TRspSelectObjectHistory,
        SelectObjectHistory)
    {
        const auto objectType = PrepareObjectType(request->object_type());
        auto match = ParseFromIdOrMeta(
            objectType,
            request->object_id(),
            request->meta());

        TAttributeSelector attributeSelector{
            FromProto<std::vector<TString>>(request->selector().paths())
        };

        std::optional<TAttributeSelector> distinctBy;

        TSelectObjectHistoryOptions options;
        std::optional<TSelectObjectHistoryContinuationToken> continuationToken;
        if (request->has_options()) {
            THROW_ERROR_EXCEPTION_IF(request->options().has_interval() && request->options().has_timestamp_interval(),
                NClient::EErrorCode::InvalidRequestArguments,
                "Only one of `interval` and `timestamp_interval` options must be set");

            FromProto(&options, request->options());
            if (request->options().has_continuation_token()) {
                typename TObjectServiceProtoModule::TSelectObjectHistoryContinuationToken protoContinuationToken;
                DeserializeContinuationToken(request->options().continuation_token(), &protoContinuationToken);
                FromProto(&continuationToken.emplace(), protoContinuationToken);
                continuationToken->SerializedToken = request->options().continuation_token();
            }

            THROW_ERROR_EXCEPTION_IF(request->options().has_distinct_by() && ! request->options().distinct(),
                "Distinct by option should be provided only when distinct option is enabled");

            if (request->options().distinct()) {
                if (request->options().has_distinct_by()) {
                    distinctBy = TAttributeSelector{
                        FromProto<std::vector<TString>>(request->options().distinct_by().paths())
                    };
                } else {
                    distinctBy = attributeSelector;
                }
            }
        }

        if (match.Uuid) {
            if (options.Uuid) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "UUID must not be both in options and in meta");
            } else {
                std::swap(options.Uuid, match.Uuid);
            }
        }
        const auto fetchRootObject = options.FetchRootObject;
        const auto format = request->format();
        auto commonOptions = GetCommonOptions(request->common_options());
        context->SetRequestInfo(
            "ObjectType: %v, ObjectKey: %v, Selector: %v, Options: %v, Format: %v, CommonOptions: %v",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
            match.Key,
            attributeSelector,
            options,
            CheckedEnumCast<NClient::NNative::EPayloadFormat>(format),
            commonOptions);

        if (format == NClient::NProto::EPayloadFormat::PF_NONE) {
            LogDeprecatedPayloadFormat(context);
        }

        auto config = Bootstrap_->GetTransactionManager()->GetConfig()->SelectObjectHistory;
        options.AllowTimeModeConversion |= config->ForceAllowTimeModeConversion;
        bool allowTimeModeConversion = options.AllowTimeModeConversion;

        TSelectObjectHistoryRequest selectObjectHistoryRequest{
            .CommonOptions = std::move(commonOptions),
            .Match = std::move(match),
            .SelectOptions = std::move(options),
            .ObjectType = objectType,
            .ContinuationToken = std::move(continuationToken)
        };
        TSelectObjectHistoryResponse result = RequestHandler_.SelectObjectHistory(
            context,
            attributeSelector,
            distinctBy,
            std::move(selectObjectHistoryRequest));

        for (auto& event : result.QueryResult.Events) {
            auto* protoEvent = response->add_events();
            Visit(event.Time,
                [] (const std::monostate&) {
                    YT_ABORT();
                },
                [&] (const TTimestamp& timestamp) {
                    protoEvent->set_timestamp(timestamp);
                    if (allowTimeModeConversion) {
                        *protoEvent->mutable_time() =
                            NProtoInterop::CastToProto(NTransactionClient::TimestampToInstant(timestamp).first);
                    }
                },
                [&] (const TInstant& time) {
                    *protoEvent->mutable_time() = NProtoInterop::CastToProto(time);
                    if (allowTimeModeConversion) {
                        protoEvent->set_timestamp(NTransactionClient::InstantToTimestamp(time).first);
                    }
                });
            ValidateEventTypeValue(event.EventTypeValue);
            protoEvent->set_event_type(static_cast<typename TObjectServiceProtoModule::EEventType>(
                event.EventTypeValue));
            protoEvent->set_user(ToProto<TProtobufString>(event.UserIdentity.User));
            protoEvent->set_user_tag(ToProto<TProtobufString>(event.UserIdentity.UserTag));
            NObjects::MoveObjectResultToProto<TObjectServiceProtoModule>(
                format,
                Bootstrap_->GetObjectManager()->GetTypeHandlerOrCrash(objectType)->GetRootProtobufType(),
                attributeSelector,
                &event.Attributes,
                Config_->ProtobufFormatUnknownFieldsMode,
                protoEvent->mutable_results(),
                fetchRootObject);
            ToProto(
                protoEvent->mutable_history_enabled_attributes(),
                event.HistoryEnabledAttributes);
            if (!event.TransactionContext.Items.empty()) {
                ToProto(
                    protoEvent->mutable_transaction_context(),
                    event.TransactionContext);
            }
        }

        {
            typename TObjectServiceProtoModule::TSelectObjectHistoryContinuationToken protoContinuationToken;
            ToProto(&protoContinuationToken, result.QueryResult.ContinuationToken);
            response->set_continuation_token(SerializeContinuationToken(protoContinuationToken));
        }
        FillPerformanceStatistics(
            response->mutable_performance_statistics(),
            result.CommonReadResult.PerformanceStatistics);
        if (result.QueryResult.LastTrimTime != TInstant::Zero()) {
            *response->mutable_last_trim_time() = NProtoInterop::CastToProto(result.QueryResult.LastTrimTime);
        }

        context->SetResponseInfo("Timestamp: %v, EventCount: %v, ContinuationToken: %v",
            result.CommonReadResult.StartTimestamp,
            response->events_size(),
            response->continuation_token());
        context->Reply();

        auto* selectObjectHistorySensors = SensorHolder_.FindOrCreateSelectObjectHistorySensors(
            context->GetMethod(),
            context->GetAuthenticationIdentity(),
            result.QueryResult.IndexUsed);

        const auto& underlyingContext = context->GetUnderlyingContext();
        TDuration totalTime = *underlyingContext->GetFinishInstant() - underlyingContext->GetArriveInstant();
        selectObjectHistorySensors->TotalTimeCounter.Record(totalTime);
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <
    class TObjectServiceProtoModule,
    class TObjectTypeRemapper>
NRpc::IServicePtr CreateObjectService(
    NMaster::IBootstrap* bootstrap,
    TObjectServiceConfigPtr config,
    const NRpc::TServiceDescriptor& descriptor)
{
    return New<NDetail::TObjectService<TObjectServiceProtoModule, TObjectTypeRemapper>>(
        bootstrap,
        std::move(config),
        descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi
