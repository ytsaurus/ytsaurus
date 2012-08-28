#pragma once

#include "public.h"

#include <ytlib/ytree/ypath_client.h>

#include <ytlib/object_client/object_service.pb.h>

namespace NYT {
namespace NObjectClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TObjectServiceProxy> TPtr;

    static Stroka GetServiceName()
    {
        return "ObjectService";
    }

    explicit TObjectServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

    //! Executes a single typed request.
    template <class TTypedRequest>
    TFuture< TIntrusivePtr<typename TTypedRequest::TTypedResponse> >
    Execute(TIntrusivePtr<TTypedRequest> innerRequest);

    //! Executes a single untyped request.
    TFuture<NBus::IMessagePtr> Execute(NBus::IMessagePtr innerRequestMessage);

    class TReqExecuteBatch;
    class TRspExecuteBatch;

    //! Mimics the types introduced by |DEFINE_RPC_PROXY_METHOD|.
    typedef TIntrusivePtr<TReqExecuteBatch> TReqExecuteBatchPtr;
    typedef TIntrusivePtr<TRspExecuteBatch> TRspExecuteBatchPtr;
    typedef TFuture<TRspExecuteBatchPtr> TInvExecuteBatch;

    //! A batched request to Cypress that holds a vector of individual requests that
    //! are transferred within a single RPC envelope.
    class TReqExecuteBatch
        : public NRpc::TClientRequest
    {
    public:
        TReqExecuteBatch(
            NRpc::IChannelPtr channel,
            const Stroka& path,
            const Stroka& verb);

        TFuture<TRspExecuteBatchPtr> Invoke();

        // Overrides base method for fluent use.
        TIntrusivePtr<TReqExecuteBatch> SetTimeout(TNullable<TDuration> timeout)
        {
            TClientRequest::SetTimeout(timeout);
            return this;
        }

        //! Adds an individual request into the batch.
        /*!
         *  Each individual request may be marked with a key.
         *  These keys can be used to retrieve the corresponding responses
         *  (thus avoiding complicated and error-prone index calculations).
         *  
         *  The client is allowed to issue an empty (|NULL|) request. This request is treated
         *  like any other and it sent to the server. The server typically sends an empty (|NULL|)
         *  response back. This feature is useful for adding dummy requests to keep
         *  the request list aligned with some other data structure.
         */
        TIntrusivePtr<TReqExecuteBatch> AddRequest(
            NYTree::TYPathRequestPtr innerRequest,
            const Stroka& key = "");

        //! Returns the current number of individual requests in the batch.
        int GetSize() const;

    private:
        typedef std::multimap<Stroka, int> TKeyToIndexes;

        NProto::TRspExecute Body;
        TKeyToIndexes KeyToIndexes;

        virtual TSharedRef SerializeBody() const;

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
        typedef std::multimap<Stroka, int> TKeyToIndexMultimap;

        TRspExecuteBatch(
            const NRpc::TRequestId& requestId,
            const TKeyToIndexMultimap& keyToIndexes);

        TFuture<TRspExecuteBatchPtr> GetAsyncResult();

        //! Returns the number of individual responses in the batch.
        int GetSize() const;

        //! Returns the individual response with a given index.
        template <class TTypedResponse>
        TIntrusivePtr<TTypedResponse> GetResponse(int index) const;

        //! Returns the individual generic response with a given index.
        NYTree::TYPathResponsePtr GetResponse(int index) const;

        //! Returns the individual generic response with a given key or NULL if no request with
        //! this key is known. At most one such response must exist.
        NYTree::TYPathResponsePtr FindResponse(const Stroka& key) const;

        //! Returns the individual generic response with a given key.
        //! Such a response must be unique.
        NYTree::TYPathResponsePtr GetResponse(const Stroka& key) const;

        //! Returns the individual response with a given key or NULL if no request with
        //! this key is known. At most one such response must exist.
        template <class TTypedResponse>
        TIntrusivePtr<TTypedResponse> FindResponse(const Stroka& key) const;

        //! Returns the individual response with a given key.
        //! Such a response must be unique.
        template <class TTypedResponse>
        TIntrusivePtr<TTypedResponse> GetResponse(const Stroka& key) const;

        //! Returns all responses with a given key (all if no key is specified).
        template <class TTypedResponse>
        std::vector< TIntrusivePtr<TTypedResponse> > GetResponses(const Stroka& key = "") const;

        //! Returns all responses with a given key (all if no key is specified).
        std::vector<NYTree::TYPathResponsePtr> GetResponses(const Stroka& key = "") const;

    private:
        TKeyToIndexMultimap KeyToIndexes;
        TPromise<TRspExecuteBatchPtr> Promise;
        NProto::TRspExecute Body;
        std::vector<int> BeginPartIndexes;

        virtual void FireCompleted();
        virtual void DeserializeBody(const TRef& data);

    };

    //! Executes a batched Cypress request.
    TReqExecuteBatchPtr ExecuteBatch();

};

////////////////////////////////////////////////////////////////////////////////


} // namespace NObjectClient
} // namespace NYT

#define OBJECT_SERVICE_PROXY_INL_H_
#include "object_service_proxy-inl.h"
#undef OBJECT_SERVICE_PROXY_INL_H_
