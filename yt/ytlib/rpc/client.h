#pragma once

#include "channel.h"

#include "../misc/property.h"
#include "../misc/delayed_invoker.h"
#include "../misc/metric.h"
#include "../misc/serialize.h"
#include "../bus/bus_client.h"
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

    TProxyBase(IChannel::TPtr channel, const Stroka& serviceName);

    DECLARE_BYVAL_RW_PROPERTY(Timeout, TDuration);

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
    DECLARE_BYVAL_RO_PROPERTY(Path, Stroka);
    DECLARE_BYVAL_RO_PROPERTY(Verb, Stroka);
    DECLARE_BYREF_RW_PROPERTY(Attachments, yvector<TSharedRef>);
    DECLARE_BYVAL_RO_PROPERTY(RequestId, TRequestId);

public:
    typedef TIntrusivePtr<TClientRequest> TPtr;

    NBus::IMessage::TPtr Serialize() const;

protected:
    IChannel::TPtr Channel;

    TClientRequest(
        IChannel* channel,
        const Stroka& path,
        const Stroka& verb);

    virtual bool SerializeBody(TBlob* data) const = 0;
    TFuture<TError>::TPtr DoInvoke(TClientResponse* response, TDuration timeout);

};

////////////////////////////////////////////////////////////////////////////////

template<
    class TRequestMessage,
    class TResponseMessage
>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
private:
    typedef TTypedClientResponse<TRequestMessage, TResponseMessage> TTypedResponse;
    TDuration Timeout;

public:
    typedef TIntrusivePtr<TTypedClientRequest> TPtr;
    typedef TFuture<typename TTypedResponse::TPtr> TInvokeResult;

    TTypedClientRequest(
        IChannel* channel,
        const Stroka& path,
        const Stroka& verb,
        TDuration timeout)
        : TClientRequest(channel, path, verb)
        , Timeout(timeout)
    {
        YASSERT(channel != NULL);
    }

    typename TInvokeResult::TPtr Invoke()
    {
        typename TInvokeResult::TPtr asyncResult = NYT::New<TInvokeResult>();
        typename TTypedResponse::TPtr response = NYT::New<TTypedResponse>(
            GetRequestId(),
            ~Channel);
        DoInvoke(~response, Timeout)->Subscribe(FromMethod(
            &TTypedClientRequest::OnReady,
            asyncResult,
            response));
        return asyncResult;
    }

    TTypedClientRequest& SetTimeout(TDuration timeout)
    {
        Timeout = timeout;
        return *this;
    }

private:
    virtual bool SerializeBody(TBlob* data) const
    {
        return SerializeProtobuf(this, data);
    }

    static void OnReady(
        TError error,
        typename TInvokeResult::TPtr asyncResult,
        typename TTypedResponse::TPtr response)
    {
        YASSERT(~asyncResult != NULL);
        YASSERT(~response != NULL);

        UNUSED(error);
        asyncResult->Set(response);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IClientResponseHandler
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IClientResponseHandler> TPtr;

    virtual void OnAcknowledgement(NBus::IBus::ESendResult sendResult) = 0;

    virtual void OnResponse(const TError& error, NBus::IMessage* message) = 0;

    virtual void OnTimeout() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TClientResponse
    : public IClientResponseHandler
{
    DECLARE_BYREF_RW_PROPERTY(RequestId, TRequestId);
    DECLARE_BYREF_RW_PROPERTY(Attachments, yvector<TSharedRef>);
    DECLARE_BYVAL_RO_PROPERTY(Error, NRpc::TError);
    DECLARE_BYVAL_RO_PROPERTY(StartTime, TInstant);

public:
    typedef TIntrusivePtr<TClientResponse> TPtr;

    NBus::IMessage::TPtr GetResponseMessage() const;

    int GetErrorCode() const;
    bool IsOK() const;

protected:
    TClientResponse(const TRequestId& requestId);

    virtual bool DeserializeBody(TRef data) = 0;

private:
    friend class TClientRequest;

    DECLARE_ENUM(EState,
        (Sent)
        (Ack)
        (Done)
    );

    // Protects state.
    TSpinLock SpinLock;
    EState State;
    NBus::IMessage::TPtr ResponseMessage;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement(NBus::IBus::ESendResult sendResult);
    virtual void OnResponse(const TError& error, NBus::IMessage* message);
    virtual void OnTimeout();

    void Deserialize(NBus::IMessage* responseMessage);
    void Complete(const TError& error);
    void Complete(int code, const Stroka& message)
    {
        return Complete(TError(code, message));
    }
};

////////////////////////////////////////////////////////////////////////////////

template<
    class TRequestMessage,
    class TResponseMessage
>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr<TTypedClientResponse> TPtr;

    TTypedClientResponse(
        const TRequestId& requestId,
        IChannel* channel)
        : TClientResponse(requestId)
    {
        YASSERT(channel != NULL);
    }

private:
    typename TFuture<TPtr>::TPtr AsyncResult;

    virtual bool DeserializeBody(TRef data)
    {
        return DeserializeProtobuf(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define RPC_DECLARE_PROXY(path, errorCodes) \
    static Stroka GetServiceName() \
    { \
        return PP_STRINGIZE(path); \
    } \
    \
    DECLARE_ENUM(E##path##Error, \
        errorCodes \
    ); \
    \
    typedef E##path##Error EErrorCode;


////////////////////////////////////////////////////////////////////////////////

#define RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::TFuture<TRsp##method::TPtr> TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return New<TReq##method>(~Channel, ServiceName, #method, GetTimeout()); \
    }

////////////////////////////////////////////////////////////////////////////////

#define USE_RPC_PROXY_METHOD(TProxy, method) \
    typedef TProxy::TReq##method TReq##method; \
    typedef TProxy::TRsp##method TRsp##method; \
    typedef TProxy::TInv##method TInv##method;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
