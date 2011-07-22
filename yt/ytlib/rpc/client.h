#pragma once

#include "common.h"
#include "message.h"
#include "rpc.pb.h"

#include "../bus/bus_client.h"
#include "../actions/async_result.h"
#include "../misc/delayed_invoker.h"

// TODO: fixme
namespace NYT
{
class TCellChannel;
}

namespace NYT {
namespace NRpc {

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

struct IChannel
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IChannel> TPtr;

    virtual TAsyncResult<TVoid>::TPtr Send(
        TIntrusivePtr<TClientRequest> request,
        TIntrusivePtr<TClientResponse> response,
        TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChannel
    : public IChannel
    , public NBus::IMessageHandler
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(NBus::TBusClient::TPtr client);
    TChannel(Stroka address);

    virtual TAsyncResult<TVoid>::TPtr Send(
        TIntrusivePtr<TClientRequest> request,
        TIntrusivePtr<TClientResponse> response,
        TDuration timeout);

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TEntry
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<TEntry> TPtr;

        TRequestId RequestId;
        TIntrusivePtr<TClientResponse> Response;
        TAsyncResult<TVoid>::TPtr Ready;
        TDelayedInvoker::TCookie TimeoutCookie;
    };

    typedef yhash_map<TRequestId, TEntry::TPtr, TRequestIdHash> TEntries;

    NBus::IBus::TPtr Bus;
    TSpinLock SpinLock;
    TEntries Entries;

    void OnAcknowledgement(
        NBus::IBus::ESendResult sendResult,
        TEntry::TPtr entry);

    bool Unregister(const TRequestId& requestId);

    TEntry::TPtr FindEntry(const TRequestId& id);

    virtual void OnMessage(
        NBus::IMessage::TPtr message,
        NBus::IBus::TPtr replyBus);

    void OnTimeout(TEntry::TPtr entry);
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
    yvector<TSharedRef> Attachments_;

    NBus::IMessage::TPtr Serialize(TRequestId requestId);
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

    TTypedClientRequest(IChannel::TPtr channel, Stroka serviceName, Stroka methodName)
        : TClientRequest(channel, serviceName, methodName)
    { }

    typename TInvokeResult::TPtr Invoke(TDuration timeout = TDuration::Zero())
    {
        typename TInvokeResult::TPtr asyncResult = new TInvokeResult();
        typename TTypedResponse::TPtr response = new TTypedResponse(Channel);
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

    EErrorCode GetErrorCode() const;

    bool IsOK() const;
    bool IsRpcError() const;
    bool IsServiceError() const;

    TRequestId GetRequestId() const;

protected:
    TClientResponse(IChannel::TPtr channel);

    virtual bool DeserializeBody(TRef data) = 0;

private:
    friend class TChannel;
    friend class TCellChannel;
    friend class TClientRequest;

    DECLARE_ENUM(EState,
        (Sent)
        (Ack)
        (Done)
    );

    // Protects state.
    TSpinLock SpinLock;
    IChannel::TPtr Channel;
    TRequestId RequestId;
    EState State;
    EErrorCode ErrorCode;
    yvector<TSharedRef> MyAttachments;

    void SetRequestId(const TRequestId& requestId);
    void Deserialize(NBus::IMessage::TPtr message);
    void Complete(EErrorCode errorCode);
    void OnAcknowledgement(NBus::IBus::ESendResult sendResult);
    void OnResponse(EErrorCode errorCode, NBus::IMessage::TPtr message);
    void OnTimeout();
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

    TTypedClientResponse(IChannel::TPtr channel)
        : TClientResponse(channel)
    { }

    TErrorCode GetErrorCode() const
    {
        return (TErrorCode) TClientResponse::GetErrorCode();
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
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ns::TRsp##method, EErrorCode> TReq##method; \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TReq##method, ns::TRsp##method, EErrorCode> TRsp##method; \
    typedef ::NYT::TAsyncResult<TRsp##method::TPtr> TInv##method; \
    \
    TReq##method::TPtr method() \
    { \
        return new TReq##method(Channel, ServiceName, #method); \
    }

////////////////////////////////////////////////////////////////////////////////

#define USE_RPC_PROXY_METHOD(TProxy, MethodName) \
    typedef TProxy::TReq##MethodName TReq##MethodName; \
    typedef TProxy::TRsp##MethodName TRsp##MethodName; \
    typedef TProxy::TInv##MethodName TInv##MethodName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
