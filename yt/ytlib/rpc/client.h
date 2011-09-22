#pragma once

#include "channel.h"

#include "../bus/bus_client.h"
#include "../actions/async_result.h"
#include "../misc/delayed_invoker.h"

// TODO: forward declaration for friends

namespace NYT {

class TCellChannel;

} // namespace NYT

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TClientRequest;

template<
    class TRequestMessage,
    class TResponseMessage
>
class TTypedClientRequest;

class TClientResponse;

template<
    class TRequestMessage,
    class TResponseMessage
>
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

    TProxyBase(IChannel::TPtr channel, Stroka serviceName);

    IChannel::TPtr Channel;
    Stroka ServiceName;
};          

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TClientRequest> TPtr;

    TRequestId GetRequestId();

    yvector<TSharedRef>& Attachments();

protected:
    IChannel::TPtr Channel;

    TClientRequest(
        IChannel::TPtr channel,
        Stroka serviceName,
        Stroka methodName);

    virtual bool SerializeBody(TBlob* data) = 0;
    TAsyncResult<TVoid>::TPtr DoInvoke(TIntrusivePtr<TClientResponse> response, TDuration timeout);

private:
    friend class TChannel;

    Stroka ServiceName;
    Stroka MethodName;
    TRequestId RequestId;

    yvector<TSharedRef> Attachments_;

    NBus::IMessage::TPtr Serialize();
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

public:
    typedef TIntrusivePtr<TTypedClientRequest> TPtr;
    typedef TAsyncResult<typename TTypedResponse::TPtr> TInvokeResult;

    TTypedClientRequest(IChannel::TPtr channel, Stroka serviceName, Stroka methodName)
        : TClientRequest(channel, serviceName, methodName)
    { }

    typename TInvokeResult::TPtr Invoke(TDuration timeout = TDuration::Zero())
    {
        typename TInvokeResult::TPtr asyncResult = NYT::New<TInvokeResult>();
        typename TTypedResponse::TPtr response = NYT::New<TTypedResponse>(
            GetRequestId(),
            Channel);
        DoInvoke(~response, timeout)->Subscribe(FromMethod(
            &TTypedClientRequest::OnReady,
            asyncResult,
            response));
        return asyncResult;
    }

private:
    virtual bool SerializeBody(TBlob* data)
    {
        return SerializeMessage(this, data);
    }

    static void OnReady(
        TVoid,
        typename TInvokeResult::TPtr asyncResult,
        typename TTypedResponse::TPtr response)
    {
        asyncResult->Set(response);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientResponse
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TClientResponse> TPtr;

    yvector<TSharedRef>& Attachments();

    TRequestId GetRequestId();

    EErrorCode GetErrorCode() const;

    bool IsOK() const;
    bool IsRpcError() const;
    bool IsServiceError() const;

protected:
    TClientResponse(
        const TRequestId& requestId,
        IChannel::TPtr channel);

    virtual bool DeserializeBody(TRef data) = 0;

private:
    friend class TChannel;
    friend class ::NYT::TCellChannel;
    friend class TClientRequest;

    DECLARE_ENUM(EState,
        (Sent)
        (Ack)
        (Done)
    );

    // Protects state.
    TSpinLock SpinLock;
    TRequestId RequestId;
    IChannel::TPtr Channel;
    EState State;
    EErrorCode ErrorCode;
    yvector<TSharedRef> MyAttachments;

    void Deserialize(NBus::IMessage::TPtr message);
    void Complete(EErrorCode errorCode);
    void OnAcknowledgement(NBus::IBus::ESendResult sendResult);
    void OnResponse(EErrorCode errorCode, NBus::IMessage::TPtr message);
    void OnTimeout();
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
        IChannel::TPtr channel)
        : TClientResponse(
            requestId,
            channel)
    { }

    EErrorCode GetErrorCode() const
    {
        return TClientResponse::GetErrorCode();
    }

private:
    typename TAsyncResult<TPtr>::TPtr AsyncResult;


    virtual bool DeserializeBody(TRef data)
    {
        return DeserializeMessage(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::TAsyncResult<TRsp##method::TPtr> TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return New<TReq##method>(Channel, ServiceName, #method); \
    }

////////////////////////////////////////////////////////////////////////////////

#define USE_RPC_PROXY_METHOD(TProxy, MethodName) \
    typedef TProxy::TReq##MethodName TReq##MethodName; \
    typedef TProxy::TRsp##MethodName TRsp##MethodName; \
    typedef TProxy::TInv##MethodName TInv##MethodName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
