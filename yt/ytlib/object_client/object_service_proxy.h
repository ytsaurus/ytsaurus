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

    //! A batched request to Cypress that holds a vector of individual requests that
    //! are transferred within a single RPC envelope.
    class TReqExecuteBatch
        : public NRpc::TClientRequest
    {
    public:
        //! Runs asynchronous invocation.
        TFuture<TRspExecuteBatchPtr> Invoke();

        //! Overrides base method for fluent use.
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
         *  like any other and it sent to the server. The server typically sends an empty (|NULL|)
         *  response back. This feature is useful for adding dummy requests to keep
         *  the request list aligned with some other data structure.
         */
        TReqExecuteBatchPtr AddRequest(
            NYTree::TYPathRequestPtr innerRequest,
            const TString& key = TString());

        //! Similar to #AddRequest, but works for already serialized messages representing requests.
        TReqExecuteBatchPtr AddRequestMessage(
            TSharedRefArray innerRequestMessage,
            const TString& key = TString());

        //! Returns the current number of individual requests in the batch.
        int GetSize() const;

    private:
        std::vector<TSharedRefArray> InnerRequestMessages_;
        std::multimap<TString, int> KeyToIndexes_;

        bool SuppressUpstreamSync_ = false;


        explicit TReqExecuteBatch(NRpc::IChannelPtr channel);
        DECLARE_NEW_FRIEND();

        virtual TSharedRef SerializeBody() const override;
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

    private:
        friend class TReqExecuteBatch;

        std::multimap<TString, int> KeyToIndexes_;
        TPromise<TRspExecuteBatchPtr> Promise_ = NewPromise<TRspExecuteBatchPtr>();
        std::vector<std::pair<int, int>> PartRanges_;

        TRspExecuteBatch(
            NRpc::TClientContextPtr clientContext,
            const std::multimap<TString, int>& keyToIndexes);
        DECLARE_NEW_FRIEND();

        void SetEmpty();

        virtual void SetPromise(const TError& error) override;
        virtual void DeserializeBody(const TRef& data) override;
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
