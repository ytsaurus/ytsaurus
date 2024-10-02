#include "generic_orm_client.h"

#include <yt/yt/orm/client/native/helpers.h>
#include <yt/yt/orm/client/native/request.h>
#include <yt/yt/orm/client/native/response.h>

#include <yt/yt/core/rpc/roaming_channel.h>

namespace NYT::NOrm::NClient::NGeneric {

////////////////////////////////////////////////////////////////////////////////

namespace {

NNative::TSelectObjectsResult ConvertSelectResponse(
    NNative::EPayloadFormat format,
    size_t selectorSize,
    bool fetchTimestamps,
    bool fetchRootObject,
    TGenericServiceProxy::TRspSelectObjectsPtr&& rsp)
{
    NClient::NNative::TSelectObjectsResult requestResult;

    requestResult.Results.reserve(rsp->results_size());
    for (auto& protoResult : *rsp->mutable_results()) {
        auto& result = requestResult.Results.emplace_back();

        FromProto(&result, std::move(protoResult));

        NNative::ValidateAttributeList(
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
    NNative::FillCommonResult(requestResult, rsp);

    return requestResult;
}

NNative::TGetObjectsResult ParseGetObjectsResult(
    NNative::EPayloadFormat format,
    size_t selectorSize,
    bool fetchTimestamps,
    bool ignoreNonexistent,
    bool fetchRootObject,
    typename TGenericServiceProxy::TRspGetObjectsPtr&& rsp)
{
    NNative::TGetObjectsResult result;

    result.Subresults.reserve(rsp->subresponses_size());
    for (auto& subresponse : rsp->subresponses()) {
        FromProto(&result.Subresults.emplace_back(), std::move(subresponse.result()));

        NNative::ValidateAttributeList(
            result.Subresults.back(),
            format,
            selectorSize,
            fetchTimestamps,
            ignoreNonexistent,
            fetchRootObject);
    }

    result.Timestamp = rsp->timestamp();
    NNative::FillCommonResult(result, rsp);

    return result;
}

class TGenericOrmClient
    : public IOrmClient
{
public:
    TGenericOrmClient(NRpc::IRoamingChannelProviderPtr channelProvider, TString serviceName)
        : ChannelProvider_(std::move(channelProvider))
        , ServiceName_(std::move(serviceName))
    { }

    TFuture<NNative::TSelectObjectsResult> SelectObjects(
        NObjects::TObjectTypeValue objectType,
        const NNative::TAttributeSelector& selector,
        const NNative::TSelectObjectsOptions& options) override
    {
        auto serviceDescriptor = NRpc::TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(NRpc::GenericProtocolVersion);
        auto channel = NConcurrency::WaitFor(ChannelProvider_->GetChannel(ServiceName_))
            .ValueOrThrow();
        TGenericServiceProxy proxy(std::move(channel), serviceDescriptor);

        auto req = proxy.SelectObjects();
        req->set_object_type(static_cast<int>(objectType));

        NClient::NNative::ToProto(req->mutable_selector(), selector);

        auto* protoOptions = req->mutable_options();
        req->set_timestamp(options.Timestamp);
        req->set_format(static_cast<typename NOrm::NClient::NProto::EPayloadFormat>(options.Format));
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
        protoOptions->set_fetch_root_object(options.FetchRootObject);
        NNative::FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(&ConvertSelectResponse,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.FetchRootObject));
    }

    TFuture<NNative::TGetObjectsResult> GetObjects(
        std::vector<NNative::TObjectIdentity> objectIdentities,
        NObjects::TObjectTypeValue objectType,
        const NNative::TAttributeSelector& selector,
        const NNative::TGetObjectOptions& options) override
    {
        auto serviceDescriptor = NYT::NRpc::TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(NYT::NRpc::GenericProtocolVersion);
        auto channel = NConcurrency::WaitFor(ChannelProvider_->GetChannel(ServiceName_))
            .ValueOrThrow();
        TGenericServiceProxy proxy(std::move(channel), serviceDescriptor);

        auto req = proxy.GetObjects();

        req->mutable_subrequests()->Reserve(objectIdentities.size());
        for (auto& objectIdentity : objectIdentities) {
            ToProto(req->add_subrequests()->mutable_meta(), std::move(objectIdentity));
        }

        req->set_object_type(static_cast<int>(objectType));
        NClient::NNative::ToProto(req->mutable_selector(), selector);
        NNative::FillCommonOptions(req, options);

        NNative::FillGetObjectRequestOptions(options, req.Get());

        return req->Invoke().ApplyUnique(BIND(&ParseGetObjectsResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.IgnoreNonexistent,
            options.FetchRootObject));
    }

    TFuture<NNative::TUpdateObjectResult> UpdateObject(
        NNative::TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        std::vector<NNative::TUpdate> updates,
        std::vector<NNative::TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
        const NNative::TUpdateObjectOptions& options) override
    {
        auto serviceDescriptor = NRpc::TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(NRpc::GenericProtocolVersion);
        auto channel = NConcurrency::WaitFor(ChannelProvider_->GetChannel(ServiceName_))
            .ValueOrThrow();
        TGenericServiceProxy proxy(std::move(channel), serviceDescriptor);

        auto req = proxy.UpdateObject();
        NNative::FillUpdateObjectRequestAttributes<int>(
            std::move(objectIdentity),
            objectType,
            std::move(updates),
            std::move(attributeTimestampPrerequisites),
            req.Get());

        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        NNative::FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (typename TGenericServiceProxy::TRspUpdateObjectPtr&& rsp)
            {
                auto result = NNative::TUpdateObjectResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                };
                NNative::FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<NNative::TCreateObjectResult> CreateObject(
        NObjects::TObjectTypeValue objectType,
        NNative::TPayload attributesPayload,
        const NNative::TCreateObjectOptions& options,
        std::optional<NNative::TUpdateIfExisting> updateIfExisting) override
    {
        auto serviceDescriptor = NYT::NRpc::TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(NYT::NRpc::GenericProtocolVersion);
        auto channel = NConcurrency::WaitFor(ChannelProvider_->GetChannel(ServiceName_))
            .ValueOrThrow();
        TGenericServiceProxy proxy(std::move(channel), serviceDescriptor);

        auto req = proxy.CreateObject();
        req->set_object_type(static_cast<int>(objectType));
        ToProto(req->mutable_attributes_payload(), std::move(attributesPayload));
        if (updateIfExisting) {
            NClient::NNative::ToProto<NProto::TUpdateIfExisting>(
                req->mutable_update_if_existing(), std::move(updateIfExisting->Updates));
            NNative::FillAttributeTimestampPrerequisites(
                req->mutable_update_if_existing(),
                std::move(updateIfExisting->AttributeTimestampPrerequisites));
        }
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_format(static_cast<NOrm::NClient::NProto::EPayloadFormat>(options.Format));
        NNative::FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (TGenericServiceProxy::TRspCreateObjectPtr&& rsp) {
                NNative::TPayload meta;
                NNative::FromProto(&meta, rsp->meta());
                auto result = NNative::TCreateObjectResult{
                    .ObjectId = std::move(*rsp->mutable_object_id()),
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .Meta = std::move(meta),
                };
                NNative::FillCommonResult(result, rsp);
                return result;
            }));
    }

    TFuture<NNative::TRemoveObjectResult> RemoveObject(
        NNative::TObjectIdentity objectIdentity,
        NObjects::TObjectTypeValue objectType,
        const NNative::TRemoveObjectOptions& options) override
    {
        auto serviceDescriptor = NYT::NRpc::TServiceDescriptor(ServiceName_)
            .SetProtocolVersion(NYT::NRpc::GenericProtocolVersion);
        auto channel = NConcurrency::WaitFor(ChannelProvider_->GetChannel(ServiceName_))
            .ValueOrThrow();
        TGenericServiceProxy proxy(std::move(channel), serviceDescriptor);

        auto req = proxy.RemoveObject();
        ToProto(req->mutable_transaction_id(), options.TransactionId);
        req->set_object_type(static_cast<int>(objectType));
        ToProto(req->mutable_meta(), std::move(objectIdentity));
        req->set_ignore_nonexistent(options.IgnoreNonexistent);
        NNative::FillCommonOptions(req, options);

        return req->Invoke().ApplyUnique(BIND(
            [] (TGenericServiceProxy::TRspRemoveObjectPtr&& rsp) {
                auto result = NNative::TRemoveObjectResult{
                    .CommitTimestamp = rsp->commit_timestamp(),
                    .FinalizationStartTime = NYT::FromProto<TInstant>(rsp->finalization_start_time()),
                };
                FillCommonResult(result, rsp);
                return result;
            }));
    }

private:
    const NRpc::IRoamingChannelProviderPtr ChannelProvider_;
    const TString ServiceName_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IOrmClientPtr CreateGenericOrmClient(NRpc::IRoamingChannelProviderPtr channelProvider, TString serviceName)
{
    return NYT::New<TGenericOrmClient>(std::move(channelProvider), std::move(serviceName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NGeneric
