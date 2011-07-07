#pragma once

#include "common.h"
#include "message.h"
#include "rpc.pb.h"

#include "../bus/bus_client.h"
#include "../actions/async_result.h"
#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
////////////////////////////////////////////////////////////////////////////////

class TClientRequest;

template<
    class TRequestMessage,
    class TResponseMessage,
    class TErrorCode = EErrorCode
>
class TTypedClientRequest;

class TClientResponse;

template<
    class TRequestMessage,
    class TResponseMessage,
    class TErrorCode = EErrorCode
>
class TTypedClientResponse;

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IMessageHandler
    , public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(TBusClient::TPtr client);

private:
    friend class TClientRequest;
    friend class TClientResponse;

    typedef yhash_map< TRequestId, TIntrusivePtr<TClientResponse>, TGUIDHash > TRequestMap;

    IBus::TPtr Bus;
    TSpinLock SpinLock;
    TRequestMap ResponseMap;

    void Send(
        TIntrusivePtr<TClientRequest> request,
        TIntrusivePtr<TClientResponse> response,
        TDuration timeout);

    TRequestId RegisterResponse(TIntrusivePtr<TClientResponse> response);
    void UnregisterResponse(TRequestId requestId);
    TIntrusivePtr<TClientResponse> GetResponse(TRequestId id);

    virtual void OnMessage(IMessage::TPtr message, IBus::TPtr replyBus);
};          

////////////////////////////////////////////////////////////////////////////////

class TChannelCache
    : private TNonCopyable
{
public:
    TChannel::TPtr GetChannel(Stroka address);

private:
    typedef yhash_map<Stroka, TChannel::TPtr> TChannelMap;

    TChannelMap ChannelMap;
};

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

    TProxyBase(TChannel::TPtr channel, Stroka serviceName);

    TChannel::TPtr Channel;
    Stroka ServiceName;
};          

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TClientRequest> TPtr;

    yvector<TSharedRef>& Attachments();

protected:
    TChannel::TPtr Channel;

    TClientRequest(
        TChannel::TPtr channel,
        Stroka serviceName,
        Stroka methodName);

    virtual bool SerializeBody(TBlob* data) = 0;
    void DoInvoke(TIntrusivePtr<TClientResponse> response, TDuration timeout);

private:
    friend class TChannel;

    Stroka ServiceName;
    Stroka MethodName;
    yvector<TSharedRef> MyAttachments;

    IMessage::TPtr Serialize(TRequestId requestId);
};

////////////////////////////////////////////////////////////////////////////////

template<
    class TRequestMessage,
    class TResponseMessage,
    class TErrorCode
>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
private:
    typedef TTypedClientResponse<TRequestMessage, TResponseMessage, TErrorCode> TTypedResponse;

public:
    typedef TIntrusivePtr<TTypedClientRequest> TPtr;
    typedef TAsyncResult<typename TTypedResponse::TPtr> TInvokeResult;

    TTypedClientRequest(TChannel::TPtr channel, Stroka serviceName, Stroka methodName)
        : TClientRequest(channel, serviceName, methodName)
    { }

    typename TInvokeResult::TPtr Invoke(TDuration timeout = TDuration::Zero())
    {
        typename TInvokeResult::TPtr asyncResult = new TInvokeResult();
        typename TTypedResponse::TPtr response = new TTypedResponse(Channel, asyncResult);
        DoInvoke(~response, timeout);
        return asyncResult;
    }

private:
    virtual bool SerializeBody(TBlob* data)
    {
        return SerializeMessage(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TClientResponse
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TClientResponse> TPtr;

    yvector<TSharedRef>& Attachments();

    EErrorCode GetErrorCode() const;

    bool IsOK() const;
    bool IsRpcError() const;
    bool IsServiceError() const;

protected:
    TClientResponse(TChannel::TPtr channel);

    virtual void SetReady() = 0;
    virtual bool DeserializeBody(TRef data) = 0;

private:
    friend class TChannel;
    friend class TClientRequest;

    enum EState
    {
        S_Sent,
        S_Ack,
        S_Done
    };

    // Protects state.
    TSpinLock SpinLock;
    TChannel::TPtr Channel;
    TRequestId RequestId;
    EState State;
    EErrorCode ErrorCode;
    yvector<TSharedRef> MyAttachments;
    TDelayedInvoker::TCookie TimeoutCookie;

    void Prepare(TRequestId requestId, TDuration timeout);
    void Deserialize(IMessage::TPtr message);
    void Complete(EErrorCode errorCode);
    void OnAcknowledgment(IBus::ESendResult sendResult);
    void OnTimeout();
    void OnResponse(EErrorCode errorCode, IMessage::TPtr message);
};

////////////////////////////////////////////////////////////////////////////////

template<
    class TRequestMessage,
    class TResponseMessage,
    class TErrorCode
>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr<TTypedClientResponse> TPtr;

    TTypedClientResponse(
        TChannel::TPtr channel,
        typename TAsyncResult<TPtr>::TPtr asyncResult)
        : TClientResponse(channel)
        , AsyncResult(asyncResult)
    { }

    TErrorCode GetErrorCode() const
    {
        return (TErrorCode) TClientResponse::GetErrorCode();
    }

private:
    typename TAsyncResult<TPtr>::TPtr AsyncResult;

    virtual void SetReady()
    {
        YASSERT(~AsyncResult != NULL);
        AsyncResult->Set(this);
        AsyncResult.Drop();
    }

    virtual bool DeserializeBody(TRef data)
    {
        return DeserializeMessage(this, data);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ns::TRsp##method, EErrorCode> TReq##method; \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TReq##method, ns::TRsp##method, EErrorCode> TRsp##method; \
    typedef ::NYT::TAsyncResult<TRsp##method::TPtr> TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return new TReq##method(Channel, ServiceName, #method); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
