#pragma once

#include "private.h"
#include "channel.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/protobuf_helpers.h>
#include <ytlib/bus/client.h>
#include <ytlib/actions/future.h>
#include <ytlib/ytree/attributes.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TProxyBase
{
protected:
    //! Service error type.
    /*!
     * Defines a basic type of error code for all proxies.
     * A derived proxy type may hide this definition by introducing
     * an appropriate descendant of NRpc::EErrorCode.
     */
    typedef NRpc::EErrorCode EErrorCode;

    TProxyBase(IChannelPtr channel, const Stroka& serviceName);

    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, DefaultTimeout);

    IChannelPtr Channel;
    Stroka ServiceName;
};          

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest
    : public virtual TRefCounted
{
    virtual NBus::IMessage::TPtr Serialize() const = 0;

    virtual const TRequestId& GetRequestId() const = 0;
    virtual const Stroka& GetPath() const = 0;
    virtual const Stroka& GetVerb() const = 0;

    virtual NYTree::IAttributeDictionary& Attributes() = 0;
    virtual const NYTree::IAttributeDictionary& Attributes() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
    DEFINE_BYREF_RW_PROPERTY(yvector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RO_PROPERTY(bool, OneWay);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, Timeout);

public:
    virtual NBus::IMessage::TPtr Serialize() const;

    virtual const TRequestId& GetRequestId() const;
    virtual const Stroka& GetPath() const;
    virtual const Stroka& GetVerb() const;

    virtual NYTree::IAttributeDictionary& Attributes();
    virtual const NYTree::IAttributeDictionary& Attributes() const;

protected:
    IChannelPtr Channel;
    Stroka Path;
    Stroka Verb;
    TRequestId RequestId;
    TAutoPtr<NYTree::IAttributeDictionary> Attributes_;

    TClientRequest(
        IChannelPtr channel,
        const Stroka& path,
        const Stroka& verb,
        bool oneWay);

    virtual TBlob SerializeBody() const = 0;

    void DoInvoke(
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout);
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
public:
    TTypedClientRequest(
        IChannelPtr channel,
        const Stroka& path,
        const Stroka& verb,
        bool oneWay)
        : TClientRequest(channel, path, verb, oneWay)
    { }

    TFuture< TIntrusivePtr<TResponse> > Invoke()
    {
        auto response = NYT::New<TResponse>(GetRequestId());
        auto asyncResult = response->GetAsyncResult();
        DoInvoke(response, Timeout_);
        return asyncResult;
    }

    // Override base method for fluent use.
    TIntrusivePtr<TTypedClientRequest> SetTimeout(TNullable<TDuration> timeout)
    {
        TClientRequest::SetTimeout(timeout);
        return this;
    }

private:
    virtual TBlob SerializeBody() const
    {
        NLog::TLogger& Logger = RpcLogger;
        TBlob blob;
        YVERIFY(SerializeToProto(this, &blob));
        return blob;
    }

};

////////////////////////////////////////////////////////////////////////////////

//! Handles response for an RPC request.
struct IClientResponseHandler
    : public virtual TRefCounted
{
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
    int GetErrorCode() const;
    bool IsOK() const;

protected:
    explicit TClientResponseBase(const TRequestId& requestId);

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
    NBus::IMessage::TPtr GetResponseMessage() const;

    NYTree::IAttributeDictionary& Attributes();
    const NYTree::IAttributeDictionary& Attributes() const;

protected:
    explicit TClientResponse(const TRequestId& requestId);

    virtual void DeserializeBody(const TRef& data) = 0;

private:
    // Protected by #SpinLock.
    NBus::IMessage::TPtr ResponseMessage;
    TAutoPtr<NYTree::IAttributeDictionary> Attributes_;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement();
    virtual void OnResponse(NBus::IMessage* message);

    void Deserialize(NBus::IMessage::TPtr responseMessage);

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr<TTypedClientResponse> TPtr;

    explicit TTypedClientResponse(const TRequestId& requestId)
        : TClientResponse(requestId)
        , Promise(NewPromise<TPtr>())
    { }

    TFuture<TPtr> GetAsyncResult()
    {
        return Promise;
    }

private:
    TPromise<TPtr> Promise;

    virtual void FireCompleted()
    {
        Promise.Set(this);
        Promise.Reset();
    }

    virtual void DeserializeBody(const TRef& data)
    {
        NLog::TLogger& Logger = RpcLogger;
        YVERIFY(DeserializeFromProto(this, data));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a one-way response.
class TOneWayClientResponse
    : public TClientResponseBase
{
public:
    typedef TIntrusivePtr<TOneWayClientResponse> TPtr;

    explicit TOneWayClientResponse(const TRequestId& requestId);

    TFuture<TPtr> GetAsyncResult();

private:
    TPromise<TPtr> Promise;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement();
    virtual void OnResponse(NBus::IMessage* message);

    virtual void FireCompleted();

};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TRsp##method> TRsp##method; \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method> TReq##method; \
    \
    typedef TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    \
    typedef ::NYT::TFuture< TRsp##method##Ptr > TInv##method; \
    \
    TReq##method##Ptr method() \
    { \
        return \
            New<TReq##method>(~Channel, ServiceName, #method, false) \
            ->SetTimeout(DefaultTimeout_); \
    }

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ONE_WAY_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayClientResponse TRsp##method; \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method> TReq##method; \
    \
    typedef TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    \
    typedef ::NYT::TFuture< TRsp##method##Ptr > TInv##method; \
    \
    TReq##method##Ptr method() \
    { \
        return \
            New<TReq##method>(~Channel, ServiceName, #method, true) \
            ->SetTimeout(DefaultTimeout_); \
    }

////////////////////////////////////////////////////////////////////////////////
} // namespace NRpc
} // namespace NYT
