#pragma once

#include "client.h"

#include "connection.h"
#include "helpers.h"
#include "peer_discovery.h"
#include "request.h"
#include "response.h"

#include <yt/yt/orm/client/objects/registry.h>
#include <yt/yt/orm/client/objects/helpers.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/yson/protobuf_interop.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <yt/yt_proto/yt/orm/client/proto/object.pb.h>

#include <google/protobuf/duration.pb.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline constexpr size_t LogCountLimitForPluralRequests = 10;

template <class TObjectServiceProxy>
class TClient
    : public IClient
{
public:
    TClient(
        IConnectionPtr connection,
        IOrmPeerDiscoveryPtr peerDiscovery,
        NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
        NObjects::IConstTagsRegistryPtr tagsRegistry,
        NObjects::IConstAccessControlRegistryPtr aclRegistry,
        TClientOptions options,
        NLogging::TLogger logger)
        : Connection_(std::move(connection))
        , PeerDiscovery_(peerDiscovery)
        , RegistriesHolder_(std::move(objectTypeRegistry), std::move(tagsRegistry), std::move(aclRegistry))
        , Options_(std::move(options))
        , Logger(std::move(logger))
    { }

private:
    const IConnectionPtr Connection_;
    const IOrmPeerDiscoveryPtr PeerDiscovery_;
    const NObjects::TRegistriesHolder RegistriesHolder_;
    const TClientOptions Options_;
    const NLogging::TLogger Logger;

    using EObjectType = TObjectServiceProxy::EObjectType;

    TFuture<TGenerateTimestampResult> GenerateTimestamp() override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel(/*retrying*/ true));
        auto req = proxy.GenerateTimestamp();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v)",
            req->GetMethod(),
            req->GetRequestId());

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspGenerateTimestampPtr&& rsp) {
                return TGenerateTimestampResult{
                    .Timestamp = rsp->timestamp()
                };
            }));
    }

    TFuture<TSelectObjectsResult> SelectObjects(
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TSelectObjectsOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(
            options.TimestampByTransactionId,
            /*retrying*/ true));
        auto req = proxy.SelectObjects();
        SetupRequest(req, /*heavy*/ true);

        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v, ObjectType: %v, Selector: %v, Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            selector,
            options);

        req->set_object_type(static_cast<EObjectType>(objectType));

        ToProto(req->mutable_selector(), selector);

        auto* protoOptions = req->mutable_options();
        req->set_timestamp(options.Timestamp);
        req->set_format(static_cast<typename NProto::EPayloadFormat>(options.Format));
        ToProto(req->mutable_timestamp_by_transaction_id(), options.TimestampByTransactionId);
        if (options.Filter) {
            req->mutable_filter()->set_query(options.Filter);
        }
        protoOptions->set_fetch_values(options.FetchValues);
        protoOptions->set_fetch_timestamps(options.FetchTimestamps);
        if (options.Offset) {
            protoOptions->set_offset(*options.Offset);
        }
        if (options.Limit) {
            protoOptions->set_limit(*options.Limit);
        }
        if (options.ContinuationToken) {
            protoOptions->set_continuation_token(*options.ContinuationToken);
        }
        if (options.Index) {
            req->mutable_index()->set_name(*options.Index);
        }

        if (options.OrderBy) {
            auto* mutableOrderBy = req->mutable_order_by();
            for (const auto& orderByExpression : *options.OrderBy) {
                auto protoExpression = mutableOrderBy->mutable_expressions()->Add();
                protoExpression->set_expression(orderByExpression.Expression);
                protoExpression->set_descending(orderByExpression.Descending);
            }
        }

        protoOptions->set_fetch_root_object(options.FetchRootObject);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(&TClient::ParseSelectObjectsResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.FetchRootObject));
    }

    TFuture<TGetObjectResult> GetObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(
            options.TransactionId,
            options.TimestampByTransactionId,
            /*retrying*/ true));
        auto req = proxy.GetObject();
        SetupRequest(req, /*heavy*/ true);

        YT_LOG_DEBUG(
            "Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectIdentity: %v, "
            "ObjectType: %v, "
            "Selector: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            objectIdentity,
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            selector,
            options);

        ToProto(req->mutable_meta(), std::move(objectIdentity));
        req->set_object_type(static_cast<EObjectType>(objectType));
        ToProto(req->mutable_selector(), selector);
        FillCommonOptions(req, options);

        FillGetObjectRequestOptions(options, req.Get());

        return req->Invoke().ApplyUnique(BIND(&TClient::ParseGetObjectResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.IgnoreNonexistent,
            options.FetchRootObject));
    }

    TFuture<TGetObjectsResult> GetObjects(
        std::vector<TObjectIdentity> objectIdentities,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(
            options.TransactionId,
            options.TimestampByTransactionId,
            /*retrying*/ true));
        auto req = proxy.GetObjects();
        SetupRequest(req, /*heavy*/ true);

        bool isTraceLogLevelEnabled = Logger.IsLevelEnabled(NLogging::ELogLevel::Trace);

        YT_LOG_DEBUG(
            "Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectIdentities: %v, "
            "ObjectType: %v, "
            "Selector: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            MakeShrunkFormattableView(
                objectIdentities,
                TDefaultFormatter(),
                isTraceLogLevelEnabled ? objectIdentities.size() : LogCountLimitForPluralRequests),
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            selector,
            options);

        req->mutable_subrequests()->Reserve(objectIdentities.size());
        for (auto& objectIdentity : objectIdentities) {
            ToProto(req->add_subrequests()->mutable_meta(), std::move(objectIdentity));
        }

        req->set_object_type(static_cast<EObjectType>(objectType));
        ToProto(req->mutable_selector(), selector);
        FillCommonOptions(req, options);

        FillGetObjectRequestOptions(options, req.Get());

        return req->Invoke().ApplyUnique(BIND(&TClient::ParseGetObjectsResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.IgnoreNonexistent,
            options.FetchRootObject));
    }

    TFuture<TUpdateObjectResult> UpdateObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        std::vector<TUpdate> updates,
        std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
        const TUpdateObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.UpdateObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectIdentity: %v, "
            "ObjectType: %v, "
            "Updates: %v, "
            "AttributeTimestampPrerequisites: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            objectIdentity,
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            MakeFormattableView(
                updates,
                [this] (TStringBuilderBase* builder, const auto& update) {
                    FormatValue(
                        builder,
                        update,
                        Logger.IsLevelEnabled(NLogging::ELogLevel::Trace) ? "" : OmitPayloadLogFormat);
                }),
            attributeTimestampPrerequisites,
            options);

        FillUpdateObjectRequestAttributes<typename TObjectServiceProxy::EObjectType>(
            std::move(objectIdentity),
            objectType,
            std::move(updates),
            std::move(attributeTimestampPrerequisites),
            req.Get());

        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspUpdateObjectPtr&& rsp) {
                auto result = TUpdateObjectResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TUpdateObjectsResult> UpdateObjects(
        std::vector<TUpdateObjectsSubrequest> updateSubrequests,
        const TUpdateObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.UpdateObjects();
        SetupRequest(req);

        bool isTraceLogLevelEnabled = Logger.IsLevelEnabled(NLogging::ELogLevel::Trace);
        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "UpdateSubrequests: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            MakeShrunkFormattableView(updateSubrequests,
                [objectTypeRegistry = GetObjectTypeRegistry(), isTraceLogLevelEnabled] (
                    TStringBuilderBase* builder, const auto& value)
                {
                    FormatValue(
                        builder,
                        value,
                        objectTypeRegistry,
                        isTraceLogLevelEnabled ? "" : OmitPayloadLogFormat);
                }, isTraceLogLevelEnabled ? updateSubrequests.size() : LogCountLimitForPluralRequests),
            options);

        req->mutable_subrequests()->Reserve(updateSubrequests.size());
        for (auto& updateSubrequest : updateSubrequests) {
            auto* subrequest = req->add_subrequests();

            FillUpdateObjectRequestAttributes<typename TObjectServiceProxy::EObjectType>(
                std::move(updateSubrequest.ObjectIdentity),
                updateSubrequest.ObjectType,
                std::move(updateSubrequest.Updates),
                std::move(updateSubrequest.AttributeTimestampPrerequisites),
                subrequest);
        }

        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspUpdateObjectsPtr&& rsp) {
                auto result = TUpdateObjectsResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TCreateObjectResult> CreateObject(
        NObjects::TObjectTypeValue objectType,
        TPayload attributesPayload,
        const TCreateObjectOptions& options,
        std::optional<TUpdateIfExisting> updateIfExisting) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.CreateObject();
        SetupRequest(req, /*heavy*/ true);

        bool isTraceLogLevelEnabled = Logger.IsLevelEnabled(NLogging::ELogLevel::Trace);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectType: %v, "
            "AttributesPayload: %v, "
            "UpdateIfExisting: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            isTraceLogLevelEnabled ? attributesPayload : TPayload{},
            ConvertToString(updateIfExisting, /*writePayload*/ isTraceLogLevelEnabled),
            options);

        req->set_object_type(static_cast<EObjectType>(objectType));
        ToProto(req->mutable_attributes_payload(), std::move(attributesPayload));
        if (updateIfExisting) {
            ToProto(req->mutable_update_if_existing(), std::move(updateIfExisting->Updates));
            FillAttributeTimestampPrerequisites(
                req->mutable_update_if_existing(),
                std::move(updateIfExisting->AttributeTimestampPrerequisites));
        }
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_format(static_cast<typename NProto::EPayloadFormat>(options.Format));
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspCreateObjectPtr&& rsp) {
                TPayload meta;
                FromProto(&meta, rsp->meta());
                auto result = TCreateObjectResult{
                    .ObjectId = std::move(*rsp->mutable_object_id()),
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .Meta = std::move(meta),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TCreateObjectsResult> CreateObjects(
        std::vector<TCreateObjectsSubrequest> subrequests,
        const TCreateObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.CreateObjects();
        SetupRequest(req, /*heavy*/ true);

        bool isTraceLogLevelEnabled = Logger.IsLevelEnabled(NLogging::ELogLevel::Trace);
        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "CreateSubrequests: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            MakeShrunkFormattableView(subrequests,
                [objectTypeRegistry = GetObjectTypeRegistry(), isTraceLogLevelEnabled] (
                    TStringBuilderBase* builder, const auto& value)
                {
                    FormatValue(
                        builder,
                        value,
                        objectTypeRegistry,
                        isTraceLogLevelEnabled ? "" : OmitPayloadLogFormat);
                }, isTraceLogLevelEnabled ? subrequests.size() : LogCountLimitForPluralRequests),
            options);

        for (const auto& createSubrequest : subrequests) {
            auto* subrequest = req->add_subrequests();
            subrequest->set_object_type(static_cast<EObjectType>(createSubrequest.ObjectType));
            ToProto(subrequest->mutable_attributes_payload(), std::move(createSubrequest.AttributesPayload));
            if (createSubrequest.UpdateIfExisting) {
                ToProto(subrequest->mutable_update_if_existing(), std::move(createSubrequest.UpdateIfExisting->Updates));
                FillAttributeTimestampPrerequisites(
                    subrequest->mutable_update_if_existing(),
                    std::move(createSubrequest.UpdateIfExisting->AttributeTimestampPrerequisites));
            }
        }
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_format(static_cast<typename NProto::EPayloadFormat>(options.Format));
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspCreateObjectsPtr&& rsp) {
                std::vector<TCreateObjectsSubresult> subresults;
                subresults.reserve(rsp->subresponses_size());
                for (auto& subresponse : *rsp->mutable_subresponses()) {
                    TPayload meta;
                    FromProto(&meta, subresponse.meta());
                    subresults.push_back(TCreateObjectsSubresult{
                        .ObjectId = std::move(*subresponse.mutable_object_id()),
                        .Fqid = std::move(*subresponse.mutable_fqid()),
                        .Meta = std::move(meta)
                    });
                }

                auto result = TCreateObjectsResult{
                    .Subresults = std::move(subresults),
                    .CommitTimestamp = rsp->commit_timestamp()
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TRemoveObjectResult> RemoveObject(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TRemoveObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.RemoveObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectIdentity: %v, "
            "ObjectType: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            objectIdentity,
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            options);

        ToProto(req->mutable_meta(), std::move(objectIdentity));
        req->set_object_type(static_cast<EObjectType>(objectType));
        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspRemoveObjectPtr&& rsp) {
                auto result = TRemoveObjectResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .FinalizationStartTime = NYT::FromProto<TInstant>(rsp->finalization_start_time()),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TRemoveObjectsResult> RemoveObjects(
        std::vector<TRemoveObjectsSubrequest> subrequests,
        const TRemoveObjectOptions& options) override
    {
        TObjectServiceProxy proxy(GetMaybeTransactionChannel(options.TransactionId));
        auto req = proxy.RemoveObjects();
        SetupRequest(req);

        bool isTraceLogLevelEnabled = Logger.IsLevelEnabled(NLogging::ELogLevel::Trace);
        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "Subrequests: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            MakeShrunkFormattableView(
                subrequests,
                [objectTypeRegistry = GetObjectTypeRegistry()] (auto* builder, const auto& value) {
                    FormatValue(builder, value, objectTypeRegistry, /*format*/ "");
                },
                isTraceLogLevelEnabled
                    ? subrequests.size()
                    : LogCountLimitForPluralRequests),
            options);

        req->mutable_subrequests()->Reserve(subrequests.size());
        for (auto& removeSubrequest : subrequests) {
            auto* subrequest = req->add_subrequests();
            subrequest->set_object_type(
                static_cast<EObjectType>(removeSubrequest.ObjectType));
            ToProto(subrequest->mutable_meta(), std::move(removeSubrequest.ObjectIdentity));
        }

        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspRemoveObjectsPtr&& rsp) {
                auto result = TRemoveObjectsResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .Subresults = NYT::FromProto<std::vector<TRemoveObjectsSubresult>>(rsp->subresponses())
                };

                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TStartTransactionResult> StartTransaction(
        const TStartTransactionOptions& options) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel(/*retrying*/ true));
        auto req = proxy.StartTransaction();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v, Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            options);

        if (options.StartTimestamp != NObjects::NullTimestamp) {
            req->set_start_timestamp(options.StartTimestamp);
        }
        if (options.UnderlyingTransactionId != NObjects::NullTransactionId) {
            ToProto(req->mutable_underlying_transaction_id(), options.UnderlyingTransactionId);
        }
        if (options.UnderlyingTransactionAddress) {
            req->set_underlying_transaction_address(options.UnderlyingTransactionAddress);
        }
        ToProto(req->mutable_mutating_transaction_options(), options.MutatingTransactionOptions);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspStartTransactionPtr&& rsp) {
                if (rsp->start_timestamp() == NObjects::NullTimestamp) {
                    THROW_ERROR_EXCEPTION("Expected non-null start timestamp");
                }
                auto transactionId = NYT::FromProto<TTransactionId>(rsp->transaction_id());
                if (!transactionId) {
                    THROW_ERROR_EXCEPTION("Expected non-null transaction id");
                }
                return TStartTransactionResult{
                    .TransactionId = transactionId,
                    .StartTimestamp = rsp->start_timestamp(),
                    .StartTime = NProtoInterop::CastFromProto(rsp->start_time()),
                };
            }));
    }

    TFuture<TAbortTransactionResult> AbortTransaction(
        TTransactionId id) override
    {
        TObjectServiceProxy proxy(GetTransactionChannel(id));
        auto req = proxy.AbortTransaction();
        SetupRequest(req);

        ToProto(req->mutable_transaction_id(), id);

        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v, TransactionId: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            req->transaction_id());
        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspAbortTransactionPtr&&) {
                return TAbortTransactionResult{};
            }));
    }

    TFuture<TCommitTransactionResult> CommitTransaction(
        TTransactionId id,
        const TCommitTransactionOptions& options) override
    {
        TObjectServiceProxy proxy(GetTransactionChannel(id));
        auto req = proxy.CommitTransaction();
        SetupRequest(req);

        ToProto(req->mutable_transaction_id(), id);
        ToProto(req->mutable_transaction_context(), options.TransactionContext);
        FillCommonOptions(req, options);

        YT_LOG_DEBUG("Invoking request (Method: %v, RequestId: %v, TransactionId: %v, Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            req->transaction_id(),
            options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspCommitTransactionPtr&& rsp) {
                TCommitTransactionResult result{
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .StartTime = NProtoInterop::CastFromProto(rsp->start_time()),
                    .FinishTime = NProtoInterop::CastFromProto(rsp->finish_time()),
                };
                FillCommonResult(result, rsp);
                FromProto(&result.TotalPerformanceStatistics, rsp->total_performance_statistics());

                return result;
            }));
    }

    TFuture<TWatchObjectsResult> WatchObjects(
        NObjects::TObjectTypeValue objectType,
        const TWatchObjectsOptions& options) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel(/*retrying*/ true));

        auto req = proxy.WatchObjects();
        SetupRequest(req, /*heavy*/ true);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectType: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            ConvertToString(options, GetTagsRegistry()));

        req->set_object_type(static_cast<EObjectType>(objectType));
        if (options.StartTimestamp != NObjects::NullTimestamp) {
            req->set_start_timestamp(options.StartTimestamp);
        } else if (options.ContinuationToken) {
            req->set_continuation_token(*options.ContinuationToken);
        } else if (options.StartFromEarliestOffset) {
            req->set_start_from_earliest_offset(options.StartFromEarliestOffset);
        } else {
            THROW_ERROR_EXCEPTION("Expected StartTimestamp, StartFromEarliestOffset or ContinuationToken in options");
        }

        if (options.Timestamp != NObjects::NullTimestamp) {
            req->set_timestamp(options.Timestamp);
        }
        if (options.EventCountLimit) {
            req->set_event_count_limit(*options.EventCountLimit);
        }
        if (options.TimeLimit) {
            auto* protoTimeLimit = req->mutable_time_limit();
            *protoTimeLimit = NProtoInterop::CastToProto(*options.TimeLimit);
        }
        if (options.ReadTimeLimit) {
            auto* protoTimeLimit = req->mutable_read_time_limit();
            *protoTimeLimit = NProtoInterop::CastToProto(*options.ReadTimeLimit);
        }
        if (options.Filter) {
            req->mutable_filter()->set_query(*options.Filter);
        }
        if (options.Selector) {
            ToProto(req->mutable_selector(), *options.Selector);
        }
        if (options.Tablets) {
            for (const auto tabletIndex : *options.Tablets) {
                req->add_tablets(tabletIndex);
            }
        }
        if (options.Format != EPayloadFormat::None) {
            req->set_format(static_cast<typename NProto::EPayloadFormat>(options.Format));
        }
        req->set_skip_trimmed(options.SkipTrimmed);
        req->mutable_common_options()->set_fetch_performance_statistics(options.FetchPerformanceStatistics);
        req->set_fetch_changed_attributes(options.FetchChangedAttributes);
        req->set_fetch_event_index(options.FetchEventIndex);
        if (options.WatchLog) {
            req->set_watch_log(options.WatchLog);
        }
        ToProto(req->mutable_required_tags(), options.RequiredTags);
        ToProto(req->mutable_excluded_tags(), options.ExcludedTags);
        FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspWatchObjectsPtr&& rsp) {
                decltype(TWatchObjectsResult::Events) events;
                FromProto(&events, *rsp);
                auto result = TWatchObjectsResult{
                    .Timestamp = rsp->timestamp(),
                    .ContinuationToken = rsp->continuation_token(),
                    .Events = events,
                    .SkippedRowCount = rsp->skipped_row_count(),
                };
                result.ChangedAttributesIndex.reserve(rsp->changed_attributes_index().size());
                for (auto& attribute_index : *rsp->mutable_changed_attributes_index()) {
                    result.ChangedAttributesIndex.push_back(std::move(*attribute_index.mutable_attribute_path()));
                }
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TSelectObjectHistoryResult> SelectObjectHistory(
        TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const TAttributeSelector& selector,
        const std::optional<TAttributeSelector>& distinctBy,
        const TSelectObjectHistoryOptions& options) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel(/*retrying*/ true));

        auto req = proxy.SelectObjectHistory();
        SetupRequest(req, /*heavy*/ true);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "RequestId: %v, "
            "ObjectIdentity: %v, "
            "ObjectType: %v, "
            "Options: %v)",
            req->GetMethod(),
            req->GetRequestId(),
            objectIdentity,
            GetObjectTypeRegistry()->FormatTypeValue(objectType),
            options);

        req->set_object_type(static_cast<EObjectType>(objectType));
        ToProto(req->mutable_meta(), std::move(objectIdentity));
        ToProto(req->mutable_selector(), selector);

        req->set_format(static_cast<typename NProto::EPayloadFormat>(options.Format));

        if (options.Limit) {
            req->mutable_options()->set_limit(*options.Limit);
        }

        if (options.ContinuationToken) {
            req->mutable_options()->set_continuation_token(*options.ContinuationToken);
        }

        if (options.TimeInterval.Begin) {
            *(req->mutable_options()->mutable_interval()->mutable_begin())
                = NProtoInterop::CastToProto(*options.TimeInterval.Begin);
        }
        if (options.TimeInterval.End) {
            *(req->mutable_options()->mutable_interval()->mutable_end())
                = NProtoInterop::CastToProto(*options.TimeInterval.End);
        }
        if (options.TimestampInterval.Begin) {
            req->mutable_options()->mutable_timestamp_interval()->set_begin(*options.TimestampInterval.Begin);
        }
        if (options.TimestampInterval.End) {
            req->mutable_options()->mutable_timestamp_interval()->set_end(*options.TimestampInterval.End);
        }
        req->mutable_options()->set_allow_time_mode_conversion(options.AllowTimeModeConversion);

        FillCommonOptions(req, options);

        req->mutable_options()->set_descending_time_order(options.DescendingTimeOrder);

        req->mutable_options()->set_distinct(options.Distinct);
        if (distinctBy) {
            ToProto(req->mutable_options()->mutable_distinct_by(), *distinctBy);
        }

        req->mutable_options()->set_fetch_root_object(options.FetchRootObject);

        req->mutable_options()->set_index_mode(static_cast<typename TObjectServiceProxy::ESelectObjectHistoryIndexMode>(options.IndexMode));

        req->mutable_options()->set_filter(options.Filter);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TObjectServiceProxy::TRspSelectObjectHistoryPtr&& rsp) {
                decltype(TSelectObjectHistoryResult::Events) events;
                FromProto(&events, *rsp);
                auto result = TSelectObjectHistoryResult{
                    .Events = std::move(events),
                    .ContinuationToken = rsp->continuation_token(),
                    .LastTrimTime =  NProtoInterop::CastFromProto(rsp->last_trim_time()),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<TGetMastersResult> GetMasters() override
    {
        return PeerDiscovery_->GetMasters(Connection_->GetChannel(/*retrying*/ true));
    }

    NObjects::IConstObjectTypeRegistryPtr GetObjectTypeRegistry() const
    {
        return RegistriesHolder_.GetObjectTypeRegistry();
    }

    NObjects::IConstTagsRegistryPtr GetTagsRegistry() const
    {
        return RegistriesHolder_.GetTagsRegistry();
    }

    void SetupRequest(const NRpc::TClientRequestPtr& request, bool heavy = false)
    {
        if (Options_.User) {
            request->SetUser(*Options_.User);
        }
        if (Options_.Token) {
            auto* credentialsExtension = request->Header().MutableExtension(
                NRpc::NProto::TCredentialsExt::credentials_ext);
            credentialsExtension->set_token(*Options_.Token);
        }
        request->SetTimeout(Connection_->GetRequestTimeout());
        request->SetResponseHeavy(heavy);
    }

    NRpc::IChannelPtr GetMaybeTransactionChannel(
        TTransactionId transactionId,
        TTransactionId timestampByTransactionId,
        bool retrying = false)
    {
        THROW_ERROR_EXCEPTION_IF(transactionId && timestampByTransactionId,
            "Only one of `transaction_id` and `timestampByTransactionId` could be specified");

        if (!transactionId) {
            transactionId = timestampByTransactionId;
        }

        return GetMaybeTransactionChannel(transactionId, retrying);
    }

    NRpc::IChannelPtr GetMaybeTransactionChannel(TTransactionId transactionId, bool retrying = false)
    {
        if (!transactionId) {
            return Connection_->GetChannel(retrying);
        }
        return GetTransactionChannel(transactionId, retrying);
    }

    NRpc::IChannelPtr GetTransactionChannel(TTransactionId transactionId, bool retrying = false)
    {
        if (!transactionId) {
            THROW_ERROR_EXCEPTION("No transaction id specified");
        }
        auto channel = Connection_->GetChannel(NObjects::InstanceTagFromTransactionId(transactionId), retrying);
        THROW_ERROR_EXCEPTION_UNLESS(channel,
            "Could not find master channel for transaction %v",
            transactionId);
        return channel;
    }

    static TSelectObjectsResult ParseSelectObjectsResult(
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        bool fetchRootObject,
        typename TObjectServiceProxy::TRspSelectObjectsPtr&& rsp)
    {
        TSelectObjectsResult requestResult;

        requestResult.Results.reserve(rsp->results_size());
        for (const auto& protoResult : rsp->results()) {
            auto& result = requestResult.Results.emplace_back();

            FromProto(&result, protoResult);

            ValidateAttributeList(
                result,
                format,
                selectorSize,
                fetchTimestamps,
                /*ignoreNonexistent*/ false,
                fetchRootObject);
        }

        requestResult.Timestamp = rsp->timestamp();

        if (rsp->has_continuation_token()) {
            requestResult.ContinuationToken = rsp->continuation_token();
        }
        FillCommonResult(requestResult, rsp);

        return requestResult;
    }

    static TGetObjectResult ParseGetObjectResult(
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        bool ignoreNonexistent,
        bool fetchRootObject,
        typename TObjectServiceProxy::TRspGetObjectPtr&& rsp)
    {
        TGetObjectResult result;

        FromProto(&result.Result, rsp->result());

        ValidateAttributeList(
            result.Result,
            format,
            selectorSize,
            fetchTimestamps,
            ignoreNonexistent,
            fetchRootObject);

        result.Timestamp = rsp->timestamp();
        FillCommonResult(result, rsp);

        return result;
    }

    static TGetObjectsResult ParseGetObjectsResult(
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        bool ignoreNonexistent,
        bool fetchRootObject,
        typename TObjectServiceProxy::TRspGetObjectsPtr&& rsp)
    {
        TGetObjectsResult result;
        result.Subresults.reserve(rsp->subresponses_size());

        for (const auto& subresponse : rsp->subresponses()) {
            FromProto(&result.Subresults.emplace_back(), subresponse.result());

            ValidateAttributeList(
                result.Subresults.back(),
                format,
                selectorSize,
                fetchTimestamps,
                ignoreNonexistent,
                fetchRootObject);
        }

        result.Timestamp = rsp->timestamp();
        FillCommonResult(result, rsp);

        return result;
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template<typename TObjectServiceProxy>
IClientPtr CreateClient(
    IConnectionPtr connection,
    NObjects::IConstObjectTypeRegistryPtr objectTypeRegistry,
    NObjects::IConstTagsRegistryPtr tagsRegistry,
    NObjects::IConstAccessControlRegistryPtr aclRegistry,
    TClientOptions options,
    NLogging::TLogger logger)
{
    auto peerDiscovery = connection->GetPeerDiscovery();
    return New<NDetail::TClient<TObjectServiceProxy>>(
        std::move(connection),
        std::move(peerDiscovery),
        std::move(objectTypeRegistry),
        std::move(tagsRegistry),
        std::move(aclRegistry),
        std::move(options),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
