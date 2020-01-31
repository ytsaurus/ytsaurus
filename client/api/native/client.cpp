#include "client.h"

#include "config.h"
#include "connection.h"
#include "object_service_proxy.h"
#include "private.h"
#include "response.h"

#include <yp/client/api/proto/object_service.pb.h>

namespace NYP::NClient::NApi::NNative {

using namespace NYT::NConcurrency;
using namespace NYT::NRpc;
using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    explicit TClient(TClientConfigPtr config)
        : Config_(std::move(config))
        , Connection_(CreateConnection(Config_->Connection))
    { }

    virtual TFuture<TGenerateTimestampResult> GenerateTimestamp() override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.GenerateTimestamp();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v)",
            req->GetMethod());

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspGenerateTimestampPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                return TGenerateTimestampResult{rsp->timestamp()};
            }));
    }

    virtual TFuture<TSelectObjectsResult> SelectObjects(
        EObjectType objectType,
        const TAttributeSelector& selector,
        const TSelectObjectsOptions& options) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.SelectObjects();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v, Selector: %v, Options: %v)",
            req->GetMethod(),
            selector,
            options);

        req->set_object_type(static_cast<NProto::EObjectType>(objectType));

        ToProto(req->mutable_selector(), selector);

        auto* protoOptions = req->mutable_options();
        req->set_timestamp(options.Timestamp);
        req->set_format(static_cast<NProto::EPayloadFormat>(options.Format));
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

        return req->Invoke().Apply(BIND(&TClient::ParseSelectObjectsResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps));
    }

    virtual TFuture<TGetObjectResult> GetObject(
        TObjectId objectId,
        EObjectType objectType,
        const TAttributeSelector& selector,
        const TGetObjectOptions& options) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.GetObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v, ObjectId: %v, ObjectType: %v, Selector: %v, Options: %v)",
            req->GetMethod(),
            objectId,
            objectType,
            selector,
            options);

        req->set_object_id(std::move(objectId));
        req->set_object_type(static_cast<NProto::EObjectType>(objectType));

        ToProto(req->mutable_selector(), selector);

        auto* protoOptions = req->mutable_options();
        req->set_timestamp(options.Timestamp);
        req->set_format(static_cast<NProto::EPayloadFormat>(options.Format));
        protoOptions->set_ignore_nonexistent(options.IgnoreNonexistent);
        protoOptions->set_fetch_values(options.FetchValues);
        protoOptions->set_fetch_timestamps(options.FetchTimestamps);

        return req->Invoke().Apply(BIND(&TClient::ParseGetObjectResult,
            options.Format,
            selector.size(),
            options.FetchTimestamps,
            options.IgnoreNonexistent));
    }

    virtual TFuture<TUpdateObjectResult> UpdateObject(
        TObjectId objectId,
        EObjectType objectType,
        std::vector<TUpdate> updates,
        std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
        const TTransactionId& transactionId) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.UpdateObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "ObjectId: %v, "
            "ObjectType: %v, "
            "Updates: %v, "
            "AttributeTimestampPrerequisites: %v, "
            "TransactionId: %v)",
            req->GetMethod(),
            objectId,
            objectType,
            updates,
            attributeTimestampPrerequisites,
            transactionId);

        req->set_object_id(std::move(objectId));
        req->set_object_type(static_cast<NProto::EObjectType>(objectType));

        for (auto& update : updates) {
            Visit(update,
                [&] (TSetUpdate& setUpdate) {
                    auto* protoSetUpdate = req->add_set_updates();
                    protoSetUpdate->set_path(std::move(setUpdate.Path));
                    ToProto(protoSetUpdate->mutable_value_payload(), setUpdate.Payload);
                    protoSetUpdate->set_recursive(setUpdate.Recursive);
                },
                [&] (TRemoveUpdate& removeUpdate) {
                    auto* protoRemoveUpdate = req->add_remove_updates();
                    protoRemoveUpdate->set_path(std::move(removeUpdate.Path));
                });
        }

        for (auto& attributeTimestampPrerequisite : attributeTimestampPrerequisites) {
            auto* protoAttributeTimestampPrerequisite = req->add_attribute_timestamp_prerequisites();
            protoAttributeTimestampPrerequisite->set_path(std::move(attributeTimestampPrerequisite.Path));
            protoAttributeTimestampPrerequisite->set_timestamp(attributeTimestampPrerequisite.Timestamp);
        }

        ToProto(req->mutable_transaction_id(), transactionId);

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspUpdateObjectPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                return TUpdateObjectResult{rsp->commit_timestamp()};
            }));
    }

    virtual TFuture<TCreateObjectResult> CreateObject(
        EObjectType objectType,
        TPayload attributesPayload,
        const TTransactionId& transactionId) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.CreateObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "ObjectType: %v, "
            "AttributesPayload: %v, "
            "TransactionId: %v)",
            req->GetMethod(),
            objectType,
            attributesPayload,
            transactionId);

        req->set_object_type(static_cast<NProto::EObjectType>(objectType));
        ToProto(req->mutable_attributes_payload(), attributesPayload);
        ToProto(req->mutable_transaction_id(), transactionId);

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspCreateObjectPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                return TCreateObjectResult{
                    std::move(*rsp->mutable_object_id()),
                    rsp->commit_timestamp()};
            }));
    }

    virtual TFuture<TRemoveObjectResult> RemoveObject(
        TObjectId objectId,
        EObjectType objectType,
        const TTransactionId& transactionId) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.RemoveObject();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request ("
            "Method: %v, "
            "ObjectId: %v, "
            "ObjectType: %v, "
            "TransactionId: %v)",
            req->GetMethod(),
            objectId,
            objectType,
            transactionId);

        req->set_object_id(std::move(objectId));
        req->set_object_type(static_cast<NProto::EObjectType>(objectType));
        ToProto(req->mutable_transaction_id(), transactionId);

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspRemoveObjectPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                return TRemoveObjectResult{
                    rsp->commit_timestamp()};
            }));
    }

    virtual TFuture<TStartTransactionResult> StartTransaction() override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.StartTransaction();
        SetupRequest(req);

        YT_LOG_DEBUG("Invoking request (Method: %v)",
            req->GetMethod());

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspStartTransactionPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                if (rsp->start_timestamp() == NullTimestamp) {
                    THROW_ERROR_EXCEPTION("Expected non-null start timestamp");
                }
                TTransactionId transactionId;
                FromProto(&transactionId, rsp->transaction_id());
                if (!transactionId) {
                    THROW_ERROR_EXCEPTION("Expected non-null transaction id");
                }
                return TStartTransactionResult{
                    std::move(transactionId),
                    rsp->start_timestamp()};
            }));
    }

    virtual TFuture<TAbortTransactionResult> AbortTransaction(
        const TTransactionId& id) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.AbortTransaction();
        SetupRequest(req);

        ToProto(req->mutable_transaction_id(), id);

        YT_LOG_DEBUG("Invoking request (Method: %v, TransactionId: %v)",
            req->GetMethod(),
            req->transaction_id());

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspAbortTransactionPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                Y_UNUSED(rsp);
                return TAbortTransactionResult{};
            }));
    }

    virtual TFuture<TCommitTransactionResult> CommitTransaction(
        const TTransactionId& id) override
    {
        TObjectServiceProxy proxy(Connection_->GetChannel());
        auto req = proxy.CommitTransaction();
        SetupRequest(req);

        ToProto(req->mutable_transaction_id(), id);

        YT_LOG_DEBUG("Invoking request (Method: %v, TransactionId: %v)",
            req->GetMethod(),
            req->transaction_id());

        return req->Invoke().Apply(BIND(
            [] (const TErrorOr<TObjectServiceProxy::TRspCommitTransactionPtr>& rspOrError) {
                const auto& rsp = rspOrError.ValueOrThrow();
                if (rsp->commit_timestamp() == NullTimestamp) {
                    THROW_ERROR_EXCEPTION("Expected non-null commit timestamp");
                }
                return TCommitTransactionResult{
                    rsp->commit_timestamp()};
            }));
    }

private:
    const TClientConfigPtr Config_;
    const IConnectionPtr Connection_;


    template <class TRequestPtr>
    void SetupRequest(const TRequestPtr& request)
    {
        Connection_->SetupRequestAuthentication(request);
        request->SetTimeout(Config_->Timeout);
    }


    static TSelectObjectsResult ParseSelectObjectsResult(
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        const TErrorOr<TObjectServiceProxy::TRspSelectObjectsPtr>& rspOrError)
    {
        const auto& rsp = rspOrError.ValueOrThrow();

        TSelectObjectsResult requestResult;

        requestResult.Results.reserve(rsp->results_size());
        for (auto& protoResult : *rsp->mutable_results()) {
            auto& result = requestResult.Results.emplace_back();

            FromProto(&result, protoResult);

            ValidateAttributeList(
                result,
                format,
                selectorSize,
                fetchTimestamps,
                /* ignoreNonexistent */ false);
        }

        requestResult.Timestamp = rsp->timestamp();

        if (rsp->has_continuation_token()) {
            requestResult.ContinuationToken = rsp->continuation_token();
        }

        return requestResult;
    }

    static TGetObjectResult ParseGetObjectResult(
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        bool ignoreNonexistent,
        const TErrorOr<TObjectServiceProxy::TRspGetObjectPtr>& rspOrError)
    {
        const auto& rsp = rspOrError.ValueOrThrow();

        TGetObjectResult result;

        FromProto(&result.Result, rsp->result());

        ValidateAttributeList(
            result.Result,
            format,
            selectorSize,
            fetchTimestamps,
            ignoreNonexistent);

        result.Timestamp = rsp->timestamp();

        return result;
    }


    static void ValidateAttributeList(
        const TAttributeList& attributeList,
        EPayloadFormat format,
        size_t selectorSize,
        bool fetchTimestamps,
        bool ignoreNonexistent)
    {
        if (selectorSize != attributeList.ValuePayloads.size()) {
            if (!(ignoreNonexistent && attributeList.ValuePayloads.empty())) {
                THROW_ERROR_EXCEPTION("Incorrect number of value payloads: expected %v, but got %v",
                    ignoreNonexistent && selectorSize
                        ? Format("%v or 0", selectorSize)
                        : Format("%v", selectorSize),
                    attributeList.ValuePayloads.size());
            }
        }

        for (const auto& valuePayload : attributeList.ValuePayloads) {
            ValidatePayloadFormat(format, valuePayload);
        }

        auto expectedTimestampCount = fetchTimestamps ? selectorSize : 0;
        if (expectedTimestampCount != attributeList.Timestamps.size()) {
            THROW_ERROR_EXCEPTION("Incorrect number of timestamps: expected %v, but got %v",
                expectedTimestampCount,
                attributeList.Timestamps.size());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(TClientConfigPtr config)
{
    return New<TClient>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi::NNative
