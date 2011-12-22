#pragma once

#include "channel.h"

#include "../misc/property.h"
#include "../misc/delayed_invoker.h"
#include "../misc/metric.h"
#include "../misc/serialize.h"
#include "../bus/client.h"
#include "../actions/future.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TClientRequest;

template <class TRequestMessage, class TResponseMessage>
class TTypedClientRequest;

class TClientResponse;

template<class TRequestMessage, class TResponseMessage>
class TTypedClientResponse;

////////////////////////////////////////////////////////////////////////////////

class TProxyBase
    : public TNonCopyable
{
protected:
    //! Service error type.
    /*!
     * Defines a basic type of error code for all proxies.
     * A derived proxy type may hide this definition by introducing
     * an appropriate descendant of NRpc::EErrorCode.
     */
    typedef NRpc::EErrorCode EErrorCode;

    TProxyBase(IChannel* channel, const Stroka& serviceName);

    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);

    IChannel::TPtr Channel;
    Stroka ServiceName;
};          

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<IClientRequest> TPtr;

    virtual NBus::IMessage::TPtr Serialize() const = 0;

    virtual TRequestId GetRequestId() const = 0;
    virtual Stroka GetPath() const = 0;
    virtual Stroka GetVerb() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Path);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Verb);
    DEFINE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);

public:
    typedef TIntrusivePtr<TClientRequest> TPtr;

    NBus::IMessage::TPtr Serialize() const;

protected:
    IChannel::TPtr Channel;

    TClientRequest(
        IChannel* channel,
        const Stroka& path,
        const Stroka& verb);

    virtual TBlob SerializeBody() const = 0;

    void DoInvoke(TClientResponse* response, TDuration timeout);

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
private:
    typedef TTypedClientResponse<TRequestMessage, TResponseMessage> TTypedResponse;
    TDuration Timeout;

public:
    typedef TTypedClientRequest<TRequestMessage, TResponseMessage> TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    TTypedClientRequest(
        IChannel* channel,
        const Stroka& path,
        const Stroka& verb)
        : TClientRequest(channel, path, verb)
    {
        YASSERT(channel);
    }

    typename TFuture< TIntrusivePtr<TTypedResponse> >::TPtr Invoke()
    {
        auto response = NYT::New< TTypedClientResponse<TRequestMessage, TResponseMessage> >(GetRequestId());
        auto asyncResult = response->GetAsyncResult();
        DoInvoke(~response, Timeout);
        return asyncResult;
    }

    TIntrusivePtr<TThis> SetTimeout(TDuration timeout)
    {
        Timeout = timeout;
        return this;
    }

private:
    virtual TBlob SerializeBody() const
    {
        NLog::TLogger& Logger = RpcLogger;
        TBlob blob;
        if (!SerializeProtobuf(this, &blob)) {
            LOG_FATAL("Error serializing request body");
        }
        return blob;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Handles response for an RPC request.
struct IClientResponseHandler
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IClientResponseHandler> TPtr;

    //! Request delivery has been acknowledged.
    virtual void OnAcknowledgement() = 0;
    //! The request has been replied with #EErrorCode::OK.
    /*!
     *  \param message A message containing the response.
     */
    virtual void OnResponse(NBus::IMessage* message) = 0;
    //! The request has failed.
    /*!
     *  \param error An error that has occurred.
     */
    virtual void OnError(const TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for both one-way and two-way responses.
class TClientResponseBase
    : public IClientResponseHandler
{
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(TError, Error);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

public:
    typedef TIntrusivePtr<TClientResponseBase> TPtr;

    int GetErrorCode() const;
    bool IsOK() const;

protected:
    TClientResponseBase(const TRequestId& requestId);

    virtual void FireCompleted() = 0;

    DECLARE_ENUM(EState,
        (Sent)
        (Ack)
        (Done)
    );

    // Protects state.
    TSpinLock SpinLock;
    EState State;

    // IClientResponseHandler implementation.
    virtual void OnError(const TError& error);

};

////////////////////////////////////////////////////////////////////////////////

//! Describes a two-way response.
class TClientResponse
    : public TClientResponseBase
{
    DEFINE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);

public:
    typedef TIntrusivePtr<TClientResponse> TPtr;

    NBus::IMessage::TPtr GetResponseMessage() const;

protected:
    friend class TClientRequest;

    TClientResponse(const TRequestId& requestId);

    virtual void DeserializeBody(const TRef& data) = 0;

private:
    // Protected by #SpinLock.
    NBus::IMessage::TPtr ResponseMessage;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement();
    virtual void OnResponse(NBus::IMessage* message);

    void Deserialize(NBus::IMessage* responseMessage);

};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr<TTypedClientResponse> TPtr;

    TTypedClientResponse(const TRequestId& requestId)
        : TClientResponse(requestId)
        , AsyncResult(NYT::New< TFuture<TPtr> >())
    { }

private:
    friend class TTypedClientRequest<TRequestMessage, TResponseMessage>;

    typename TFuture<TPtr>::TPtr AsyncResult;

    typename TFuture<TPtr>::TPtr GetAsyncResult()
    {
        return AsyncResult;
    }

    virtual void FireCompleted()
    {
        AsyncResult->Set(this);
        AsyncResult.Reset();
    }

    virtual void DeserializeBody(const TRef& data)
    {
        NLog::TLogger& Logger = RpcLogger;
        if (!DeserializeProtobuf(this, data)) {
            LOG_FATAL("Error deserializing response body");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a one-way response.
class TOneWayClientResponse
    : public TClientResponseBase
{
public:
    typedef TIntrusivePtr<TOneWayClientResponse> TPtr;

protected:
    friend class TClientRequest;

    TFuture<TPtr>::TPtr AsyncResult;

    TOneWayClientResponse(const TRequestId& requestId);

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement();
    virtual void OnResponse(NBus::IMessage* message);

    TFuture<TPtr>::TPtr GetAsyncResult();
    virtual void FireCompleted();

};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::TFuture<TRsp##method::TPtr> TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return \
            New<TReq##method>(~Channel, ServiceName, #method) \
            ->SetTimeout(Timeout_); \
    }

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ONE_WAY_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ::NYT::NRpc::TOneWayClientResponse::TPtr> TReq##method; \
    typedef ::NYT::NRpc::TOneWayClientResponse::TPtr TRsp##method; \
    typedef ::NYT::TFuture< ::NYT::NRpc::TOneWayClientResponse::TPtr > TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return \
            New<TReq##method>(~Channel, ServiceName, #method) \
            ->SetTimeout(Timeout_); \
    }

////////////////////////////////////////////////////////////////////////////////
} // namespace NRpc
} // namespace NYT
