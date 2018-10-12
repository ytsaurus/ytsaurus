#pragma once

#include "public.h"

#include <yt/ytlib/object_client/object_service.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/ytree/ypath_client.h>

namespace NYT {
namespace NObjectClient {

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

    class TReqExecuteBatch;
    class TRspExecuteBatch;

    //! Mimics the types introduced by |DEFINE_RPC_PROXY_METHOD|.
    using TReqExecuteBatchPtr = TIntrusivePtr<TReqExecuteBatch>;
    using TRspExecuteBatchPtr = TIntrusivePtr<TRspExecuteBatch>;
    using TErrorOrRspExecuteBatchPtr = TErrorOr<TRspExecuteBatchPtr>;

private:
    class TReqExecuteSubbatch
        : public NRpc::TClientRequest
    {
    public:
        //! Returns the current number of individual requests in the batch.
        int GetSize() const;

        virtual size_t GetHash() const override;

    protected:
        explicit TReqExecuteSubbatch(NRpc::IChannelPtr channel);

    private:
        // For TReqExecuteBatch::Slice().
        TReqExecuteSubbatch(
            const TReqExecuteBatch& other,
            int beginPos,
            int retriesEndPos,
            int endPos);
        DECLARE_NEW_FRIEND();

        virtual TSharedRef SerializeBody() const override;

        // A template method that uses the virtual function below in its
        // implementation.
        TFuture<TRspExecuteBatchPtr> DoInvoke();

        virtual TRspExecuteBatchPtr CreateResponse();

        //! Patch the message and set the 'retry' flag to true.
        static TSharedRefArray PatchForRetry(const TSharedRefArray& message);

        struct TInnerRequestDescriptor
        {
            TSharedRefArray Message;
            // True if the message contains a mutating request and has the
            // 'retry' flag set to false.
            bool NeedsPatchingForRetry = false;
            TNullable<size_t> Hash;
        };

        std::vector<TInnerRequestDescriptor> InnerRequestDescriptors_;
        bool SuppressUpstreamSync_ = false;

        friend class TReqExecuteBatch;
    };

    using TReqExecuteSubbatchPtr = TIntrusivePtr<TReqExecuteSubbatch>;

public:
    //! A batched request to Cypress that holds a vector of individual requests.
    //! They're sent in groups of several requests at a time. These groups are
    //! called subbatches and are transferred within a single RPC envelope.
    class TReqExecuteBatch
        : public TReqExecuteSubbatch
    {
    public:
        static const int MaxSingleSubbatchSize = 100;

        //! Runs asynchronous invocation.
        TFuture<TRspExecuteBatchPtr> Invoke();

        //! Overrides base method for fluent use.
        //! NB: the timeout only affects subbatch requests. The complete batch request
        //! time is essentially unbounded.
        TReqExecuteBatchPtr SetTimeout(TNullable<TDuration> timeout);

        //! Sets the upstream sync suppression option.
        TReqExecuteBatchPtr SetSuppressUpstreamSync(bool value);

        //! Adds an individual request into the batch.
        /*!
         *  Each individual request may be marked with a key.
         *  These keys can be used to retrieve the corresponding responses
         *  (thus avoiding complicated and error-prone index calculations).
         *
         *  The client is allowed to issue an empty (|nullptr|) request. This request is treated
         *  like any other and is sent to the server. The server typically sends an empty (|NULL|)
         *  response back. This feature is useful for adding dummy requests to keep
         *  the request list aligned with some other data structure.
         */
        TReqExecuteBatchPtr AddRequest(
            NYTree::TYPathRequestPtr innerRequest,
            const TString& key = TString(),
            TNullable<size_t> hash = Null);

        //! Similar to #AddRequest, but works for already serialized messages representing requests.
        //! #needsPatchingForRetry should be true iff the message contains a mutating request with
        //! the 'retry' flag set to false.
        TReqExecuteBatchPtr AddRequestMessage(
            TSharedRefArray innerRequestMessage,
            bool needsPatchingForRetry,
            const TString& key = TString(),
            TNullable<size_t> hash = Null);

    private:
        std::multimap<TString, int> KeyToIndexes_;

        // The promise is needed right away as we need to return something to
        // the caller of #Invoke. The full response, however, may not be
        // required if the first subbatch response carries all subresponses. In
        // that case, the promise will be signaled with just that subbatch
        // response.
        TPromise<TRspExecuteBatchPtr> FullResponsePromise_;
        TRspExecuteBatchPtr FullResponse_;

        // Indexes of the first and the last subrequests in the currently
        // invoked subbatch.
        int CurBatchBegin_ = 0;
        int CurBatchEnd_ = 0;

        TFuture<TObjectServiceProxy::TRspExecuteBatchPtr> CurReqFuture_;

        explicit TReqExecuteBatch(NRpc::IChannelPtr channel);
        DECLARE_NEW_FRIEND();

        //! Returns a slice of this request.
        /*!
         *  The slice is a batch request that has just the [#beginPos, #endPos)
         *  subrequests copied. The metadata is almost the same, but not
         *  quite. Notably, the key-to-index mapping is not copied, and the ID
         *  of the request is generated anew. Also, [#beginPos, #retriesEndPos)
         *  requests are marked as retries.
         */
        TReqExecuteSubbatchPtr Slice(int beginPos, int retriesEndPos, int endPos);

        virtual TRspExecuteBatchPtr CreateResponse() override;

        void PushDownPrerequisites();

        void InvokeNextBatch();
        void OnSubbatchResponse(const TErrorOr<TRspExecuteBatchPtr>& rspOrErr);

        int GetTotalSubrequestCount() const;
        int GetTotalSubresponseCount() const;

        TRspExecuteBatchPtr& FullResponse();

        friend class TReqExecuteSubbatch;
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

        //! Returns the number of individual responses in the batch.
        int GetSize() const;

        //! Returns the individual response with a given index.
        template <class TTypedResponse>
        TErrorOr<TIntrusivePtr<TTypedResponse>> GetResponse(int index) const;

        //! Returns the individual generic response with a given index.
        TErrorOr<NYTree::TYPathResponsePtr> GetResponse(int index) const;

        //! Returns the individual generic response with a given key or NULL if no request with
        //! this key is known. At most one such response must exist.
        TNullable<TErrorOr<NYTree::TYPathResponsePtr>> FindResponse(const TString& key) const;

        //! Returns the individual generic response with a given key.
        //! Such a response must be unique.
        TErrorOr<NYTree::TYPathResponsePtr> GetResponse(const TString& key) const;

        //! Returns the individual response with a given key or NULL if no request with
        //! this key is known. At most one such response must exist.
        template <class TTypedResponse>
        TNullable<TErrorOr<TIntrusivePtr<TTypedResponse>>> FindResponse(const TString& key) const;

        //! Returns the individual response with a given key.
        //! Such a response must be unique.
        template <class TTypedResponse>
        TErrorOr<TIntrusivePtr<TTypedResponse>> GetResponse(const TString& key) const;

        //! Returns all responses with a given key (all if no key is specified).
        template <class TTypedResponse>
        std::vector<TErrorOr<TIntrusivePtr<TTypedResponse>>> GetResponses(const TString& key = TString()) const;

        //! Returns all responses with a given key (all if no key is specified).
        std::vector<TErrorOr<NYTree::TYPathResponsePtr>> GetResponses(const TString& key = TString()) const;

        //! Similar to #GetResponse, but returns the response message without deserializing it.
        TSharedRefArray GetResponseMessage(int index) const;

        //! Returns revision of specified response.
        TNullable<i64> GetRevision(int index) const;

    private:
        friend class TReqExecuteSubbatch;
        friend class TReqExecuteBatch;

        std::multimap<TString, int> KeyToIndexes_;
        TPromise<TRspExecuteBatchPtr> Promise_ = NewPromise<TRspExecuteBatchPtr>();
        std::vector<std::pair<int, int>> PartRanges_;
        std::vector<i64> Revisions_;

        TRspExecuteBatch(
            NRpc::TClientContextPtr clientContext,
            const std::multimap<TString, int>& keyToIndexes);

        TRspExecuteBatch(
            NRpc::TClientContextPtr clientContext,
            const std::multimap<TString, int>& keyToIndexes,
            TPromise<TRspExecuteBatchPtr> promise);

        DECLARE_NEW_FRIEND();

        void SetEmpty();

        virtual void SetPromise(const TError& error) override;
        virtual void DeserializeBody(const TRef& data) override;

        void Append(const TRspExecuteBatchPtr& subbatchResponse);
    };

    //! Executes a batched Cypress request.
    TReqExecuteBatchPtr ExecuteBatch();
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
    const TString& key = TString());

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectClient
} // namespace NYT

#define OBJECT_SERVICE_PROXY_INL_H_
#include "object_service_proxy-inl.h"
#undef OBJECT_SERVICE_PROXY_INL_H_
