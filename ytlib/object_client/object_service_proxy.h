#pragma once

#include "public.h"

#include <yt/ytlib/object_client/proto/object_service.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/hydra/public.h>

#include <yt/core/misc/optional.h>

#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

static constexpr int DefaultSubbatchSize = 100;

////////////////////////////////////////////////////////////////////////////////

class TReqExecuteBatchWithRetriesConfig
    : public NYTree::TYsonSerializable
{
public:
    // Since #TErrorCode is an opaque |int|, let's use |int| for serialization
    std::vector<TErrorCode::TUnderlying> RetriableErrorCodes;
    TDuration StartBackoff;
    TDuration MaxBackoff;
    double BackoffMultiplier;
    int RetryCount;

    TReqExecuteBatchWithRetriesConfig()
    {
        RegisterParameter("retriable_errors", RetriableErrorCodes)
            .Default({});
        RegisterParameter("base_backoff", StartBackoff)
            .Default(TDuration::Seconds(1));
        RegisterParameter("max_backoff", MaxBackoff)
            .Default(TDuration::Seconds(20));
        RegisterParameter("backoff_multiplier", BackoffMultiplier)
            .GreaterThanOrEqual(1)
            .Default(2);
        RegisterParameter("retry_count", RetryCount)
            .GreaterThanOrEqual(0)
            .Default(5);
    }
};

DEFINE_REFCOUNTED_TYPE(TReqExecuteBatchWithRetriesConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TObjectServiceProxy, ObjectService,
        .SetProtocolVersion(12));

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);
    DEFINE_RPC_PROXY_METHOD(NProto, GCCollect);

    //! Executes a single typed request.
    template <class TTypedRequest>
    TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
    Execute(TIntrusivePtr<TTypedRequest> innerRequest);

    class TReqExecuteBatchBase;
    class TReqExecuteBatch;
    class TReqExecuteBatchWithRetries;
    class TRspExecuteBatch;

    //! Mimics the types introduced by |DEFINE_RPC_PROXY_METHOD|.
    using TReqExecuteBatchBasePtr = TIntrusivePtr<TReqExecuteBatchBase>;
    using TReqExecuteBatchPtr = TIntrusivePtr<TReqExecuteBatch>;
    using TReqExecuteBatchWithRetriesPtr = TIntrusivePtr<TReqExecuteBatchWithRetries>;
    using TRspExecuteBatchPtr = TIntrusivePtr<TRspExecuteBatch>;
    using TErrorOrRspExecuteBatchPtr = TErrorOr<TRspExecuteBatchPtr>;

private:

    class TReqExecuteSubbatch
        : public NRpc::TClientRequest
    {
    public:
        struct TInnerRequestDescriptor
        {
            std::optional<TString> Key;
            std::any Tag;
            TSharedRefArray Message;
            std::optional<size_t> Hash;
        };

        //! Returns the current number of individual requests in the batch.
        int GetSize() const;

        virtual size_t GetHash() const override;

    protected:
        std::vector<TInnerRequestDescriptor> InnerRequestDescriptors_;
        NRpc::TRequestId OriginalRequestId_;
        bool SuppressUpstreamSync_ = false;

        explicit TReqExecuteSubbatch(NRpc::IChannelPtr channel, int subbatchSize);
        explicit TReqExecuteSubbatch(const TReqExecuteSubbatch& other);
        TReqExecuteSubbatch(
            const TReqExecuteSubbatch& other,
            std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors);

        TFuture<TRspExecuteBatchPtr> DoInvoke();

        int SubbatchSize_;

    private:
        friend class TReqExecuteBatch;

        DECLARE_NEW_FRIEND();

        virtual TSharedRefArray SerializeData() const override;
    };

    using TReqExecuteSubbatchPtr = TIntrusivePtr<TReqExecuteSubbatch>;

public:
    class TReqExecuteBatchBase
        : public TReqExecuteSubbatch
    {
    public:
        //! Sets the original request id (for diagnostics only).
        void SetOriginalRequestId(NRpc::TRequestId originalRequestId);

        //! Sets the upstream sync suppression option.
        void SetSuppressUpstreamSync(bool value);

        //! Adds an individual request into the batch.
        /*!
         *  Each individual request may be marked with a key.
         *  These keys can be used to retrieve the corresponding responses
         *  (thus avoiding complicated and error-prone index calculations).
         *  the request list aligned with some other data structure.
         */
         void AddRequest(
            const NYTree::TYPathRequestPtr& innerRequest,
            std::optional<TString> key = std::nullopt,
            std::optional<size_t> hash = std::nullopt);

        //! Similar to #AddRequest, but works for already serialized messages representing requests.
        void AddRequestMessage(
            TSharedRefArray innerRequestMessage,
            std::optional<TString> key = std::nullopt,
            std::any tag = {},
            std::optional<size_t> hash = std::nullopt);

        //! Invokes the batch request. Beware: this doesn't retry back offs and uncertain indexes.
        //! Instead, consider using TReqExecuteBatch or even TReqExecuteBatchWithRetries.
        TFuture<TRspExecuteBatchPtr> Invoke();

    protected:
        explicit TReqExecuteBatchBase(NRpc::IChannelPtr channel, int subbatchSize);
        explicit TReqExecuteBatchBase(const TReqExecuteBatchBase& other);
        TReqExecuteBatchBase(const TReqExecuteBatchBase& other, std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors);

        DECLARE_NEW_FRIEND();

        void PushDownPrerequisites();

        // The promise is needed right away as we need to return something to
        // the caller of #Invoke. The full response, however, may not be
        // required if the first subbatch response carries all subresponses. In
        // that case, the promise will be signaled with just that subbatch
        // response.
        TPromise<TRspExecuteBatchPtr> FullResponsePromise_;
        TRspExecuteBatchPtr FullResponse_;
    };

    //! A batched request to Cypress that holds a vector of individual requests.
    //! They're sent in groups of several requests at a time. These groups are
    //! called subbatches and are transferred within a single RPC envelope.
    class TReqExecuteBatch
        : public TReqExecuteBatchBase
    {
    public:
        //! Starts the asynchronous invocation.
        TFuture<TRspExecuteBatchPtr> Invoke();

    protected:
        explicit TReqExecuteBatch(NRpc::IChannelPtr channel, int subbatchSize);
        TReqExecuteBatch(const TReqExecuteBatchBase& other, std::vector<TInnerRequestDescriptor>&& innerRequestDescriptors);

    private:
        TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> CurrentReqFuture_;
        bool IsFirstBatch_ = true;

        DECLARE_NEW_FRIEND();

        //! Patch the message and set the 'retry' flag to true.
        static TSharedRefArray PatchForRetry(const TSharedRefArray& message);

        TReqExecuteSubbatchPtr FormNextBatch();

        void InvokeNextBatch();
        void OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr);

        int GetTotalSubrequestCount() const;
        int GetFirstUnreceivedSubresponseIndex() const;
        bool IsSubresponseUncertain(int index) const;
        bool IsSubresponseReceived(int index) const;

        TRspExecuteBatchPtr GetFullResponse();
    };

    class TReqExecuteBatchWithRetries
        : public TReqExecuteBatchBase
    {
    public:
        //! Starts the asynchronous invocation.
        TFuture<TRspExecuteBatchPtr> Invoke();

    private:
        // For testing purposes
        TReqExecuteBatchWithRetries(
            NRpc::IChannelPtr channel,
            TReqExecuteBatchWithRetriesConfigPtr config,
            TCallback<bool(int, const TError&)>,
            int subbatchSize = DefaultSubbatchSize);
        TReqExecuteBatchWithRetries(
            NRpc::IChannelPtr channel,
            TReqExecuteBatchWithRetriesConfigPtr config,
            int subbatchSize = DefaultSubbatchSize);

        DECLARE_NEW_FRIEND();

        void InvokeNextBatch();

        void Initialize();
        void OnBatchResponse(const TErrorOr<TRspExecuteBatchPtr>& batchRspOrErr);
        void OnRetryDelayFinished();
        bool IsRetryNeeded(const TError& err);
        TDuration GetCurrentDelay();


        static TSharedRefArray PatchMutationId(const TSharedRefArray& message);

        std::vector<int> PendingIndexes_;
        int CurrentRetry_ = 0;

        TFuture<TRspExecuteBatchPtr> CurrentReqFuture_;
        TReqExecuteBatchWithRetriesConfigPtr Config_;
        TCallback<bool(const TError&)> NeedRetry_;
    };

    //! A response to a batched request.
    /*!
     *  This class holds a vector of messages representing responses to individual
     *  requests that were earlier sent to Cypress.
     *
     *  The length of this vector (see #GetSize) coincides to that of the requests vector.
     *
     *  Individual responses can be extracted by calling #GetResponse. Since they may be of
     *  different actual types, the client must supply an additional type parameter.
     *  Responses may also be retrieved by specifying a key that was used during
     *  request insertion.
     *
     */
    class TRspExecuteBatch
        : public NRpc::TClientResponse
    {
    public:
        TPromise<TRspExecuteBatchPtr> GetPromise();

        //! Returns the number of individual subrequests in the corresponding batch request.
        //! This is the same as the maximum number of responses. The actual number of
        //! responses may be lower because of backoff and uncertain requests.
        int GetSize() const;

        //! Returns the number of individual responses that have actually been received.
        int GetResponseCount() const;

        //! Returns the indexes of subrequests that could have started executing but
        //! for which no reply was received.
        std::vector<int> GetUncertainRequestIndexes() const;

        TGuid GetRequestId() const;

        //! Returns the individual response with a given index.
        template <class TTypedResponse>
        TErrorOr<TIntrusivePtr<TTypedResponse>> GetResponse(int index) const;

        //! Returns the individual generic response with a given index.
        TErrorOr<NYTree::TYPathResponsePtr> GetResponse(int index) const;

        //! Returns the individual generic response with a given key or |nullptr| if no request with
        //! this key is known. At most one such response must exist, otherwise and exception is thrown.
        std::optional<TErrorOr<NYTree::TYPathResponsePtr>> FindResponse(const TString& key) const;

        //! Returns the individual generic response with a given key.
        //! Such a response must be unique.
        TErrorOr<NYTree::TYPathResponsePtr> GetResponse(const TString& key) const;

        //! Returns the individual response with a given key or NULL if no request with
        //! this key is known. At most one such response must exist.
        template <class TTypedResponse>
        std::optional<TErrorOr<TIntrusivePtr<TTypedResponse>>> FindResponse(const TString& key) const;

        //! Returns the individual response with a given key.
        //! Such a response must be unique.
        template <class TTypedResponse>
        TErrorOr<TIntrusivePtr<TTypedResponse>> GetResponse(const TString& key) const;

        //! Returns all responses with a given key (all if no key is specified).
        template <class TTypedResponse>
        std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> GetResponses(const std::optional<TString>& key = std::nullopt) const;

        //! Returns all responses with a given key (all if no key is specified).
        std::vector<TErrorOr<NYTree::TYPathResponsePtr>> GetResponses(const std::optional<TString>& key = {}) const;

        //! Similar to #GetResponse, but returns the response message without deserializing it.
        TSharedRefArray GetResponseMessage(int index) const;

        //! Returns the revision of the specified response.
        NHydra::TRevision GetRevision(int index) const;

    private:
        friend class TReqExecuteSubbatch;
        friend class TReqExecuteBatch;
        friend class TReqExecuteBatchWithRetries;

        struct TInnerRequestDescriptor
        {
            std::optional<TString> Key;
            std::any Tag;
        };

        struct TResponseMeta
        {
            std::pair<int, int> PartRange;
            NHydra::TRevision Revision;
        };

        struct TInnerResponseDescriptor
        {
            bool Uncertain = false;
            std::optional<TResponseMeta> Meta;
        };

        struct TAttachmentRange
        {
            std::vector<TSharedRef>::const_iterator Begin;
            std::vector<TSharedRef>::const_iterator End;
        };

        std::vector<TInnerRequestDescriptor> InnerRequestDescriptors_;
        std::vector<TInnerResponseDescriptor> InnerResponseDescriptors_;
        int ResponseCount_ = 0; // the number of items in InnerResponseDescriptors_ with non-null meta
        TPromise<TRspExecuteBatchPtr> Promise_ = NewPromise<TRspExecuteBatchPtr>();
        int FirstUnreceivedResponseIndex_ = 0;

        explicit TRspExecuteBatch(
            NRpc::TClientContextPtr clientContext,
            const std::vector<TReqExecuteSubbatch::TInnerRequestDescriptor>& innerRequestDescriptors,
            TPromise<TRspExecuteBatchPtr> promise = {});

        DECLARE_NEW_FRIEND();

        void SetEmpty();

        virtual void SetPromise(const TError& error) override;
        virtual void DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = std::nullopt) override;

        // A response may be either received or unreceived.
        // An unreceived response may also be uncertain or not.
        void SetResponseReceived(int index, NHydra::TRevision revision, TAttachmentRange attachments);
        void SetResponseUncertain(int index);

        bool IsResponseReceived(int index) const;
        bool IsResponseUncertain(int index) const;

        int GetFirstUnreceivedResponseIndex() const;

        //! Returns a range of attachments for the specified response. The response must be received.
        TAttachmentRange GetResponseAttachmentRange(int index) const;
    };

    //! Executes a batched Cypress request. Retries backed off and uncertain
    //! subrequests. May take an arbitrarily long time.
    TReqExecuteBatchPtr ExecuteBatch(int subbatchSize = DefaultSubbatchSize);

    //! Executes a single batch RPC request. Results in a (batch) response that
    //! may contain unreplied subresponses (including uncertain ones).
    TReqExecuteBatchBasePtr ExecuteBatchNoBackoffRetries(int subbatchSize = DefaultSubbatchSize);

    //! Same as ExecuteBatch, but additionally retries any subrequest that results a retriable error.
    TReqExecuteBatchWithRetriesPtr ExecuteBatchWithRetries(TReqExecuteBatchWithRetriesConfigPtr config, int subbatchSize = DefaultSubbatchSize);
    TReqExecuteBatchWithRetriesPtr ExecuteBatchWithRetries(
        TReqExecuteBatchWithRetriesConfigPtr config,
        TCallback<bool(int, const TError&)> needRetry,
        int subbatchSize = DefaultSubbatchSize);

    template <class TBatchReqPtr>
    void PrepareBatchRequest(const TBatchReqPtr& batchReq);
};

////////////////////////////////////////////////////////////////////////////////

//! Returns the cumulative error for the whole batch.
/*!
 *  If the envelope request fails then the corresponding error is returned.
 *  Otherwise, individual responses are examined and a cumulative error
 *  is constructed (with individual errors attached as inner).
 *  If all individual responses were successful then OK is returned.
 *  If |key| is specified, only the responses marked with corresponding |key| are considered.
 */
TError GetCumulativeError(
    const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError,
    const std::optional<TString>& key = {});

//! Similar to the above but the envelope request is known to be successful.
TError GetCumulativeError(
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const std::optional<TString>& key = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

#define OBJECT_SERVICE_PROXY_INL_H_
#include "object_service_proxy-inl.h"
#undef OBJECT_SERVICE_PROXY_INL_H_
