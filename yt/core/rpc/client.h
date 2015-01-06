#pragma once

#include "public.h"
#include "channel.h"

#include <core/misc/property.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/delayed_executor.h>

#include <core/compression/public.h>

#include <core/bus/client.h>

#include <core/rpc/helpers.h>
#include <core/rpc/rpc.pb.h>

#include <core/actions/future.h>

#include <core/logging/log.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest
    : public virtual TRefCounted
{
    virtual TSharedRefArray Serialize() = 0;

    virtual const NProto::TRequestHeader& Header() const = 0;
    virtual NProto::TRequestHeader& Header() = 0;

    virtual bool IsOneWay() const = 0;
    virtual bool IsRequestHeavy() const = 0;
    virtual bool IsResponseHeavy() const = 0;

    virtual TRequestId GetRequestId() const = 0;

    virtual const Stroka& GetService() const = 0;
    virtual const Stroka& GetMethod() const = 0;

    virtual TInstant GetStartTime() const = 0;
    virtual void SetStartTime(TInstant value) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientRequest)

////////////////////////////////////////////////////////////////////////////////

class TClientContext
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(NTracing::TTraceContext, TraceContext);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Service);
    DEFINE_BYVAL_RO_PROPERTY(Stroka, Method);

public:
    TClientContext(
        const TRequestId& requestId,
        const NTracing::TTraceContext& traceContext,
        const Stroka& service,
        const Stroka& method)
        : RequestId_(requestId)
        , TraceContext_(traceContext)
        , Service_(service)
        , Method_(method)
    { }
};

DEFINE_REFCOUNTED_TYPE(TClientContext)

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
public:
    DEFINE_BYREF_RW_PROPERTY(NProto::TRequestHeader, Header);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, RequestAck);
    DEFINE_BYVAL_RW_PROPERTY(bool, RequestHeavy);
    DEFINE_BYVAL_RW_PROPERTY(bool, ResponseHeavy);

public:
    virtual TSharedRefArray Serialize() override;

    virtual bool IsOneWay() const override;
    
    virtual TRequestId GetRequestId() const override;

    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetMethod() const override;

    virtual TInstant GetStartTime() const override;
    virtual void SetStartTime(TInstant value) override;

protected:
    IChannelPtr Channel_;

    TSharedRef SerializedBody_;

    TClientRequest(
        IChannelPtr channel,
        const Stroka& service,
        const Stroka& method,
        bool oneWay,
        int protocolVersion);

    virtual bool IsRequestHeavy() const;
    virtual bool IsResponseHeavy() const;
    virtual TSharedRef SerializeBody() const = 0;

    TClientContextPtr CreateClientContext();

    void DoInvoke(IClientResponseHandlerPtr responseHandler);

};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern const NLog::TLogger RpcClientLogger;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
public:
    typedef decltype(((TResponse*) nullptr)->GetAsyncResult()) TInvokeResult;
    typedef TIntrusivePtr<TTypedClientRequest> TThisPtr;

    TTypedClientRequest(
        IChannelPtr channel,
        const Stroka& path,
        const Stroka& method,
        bool oneWay,
        int protocolVersion)
        : TClientRequest(
            std::move(channel),
            path,
            method,
            oneWay,
            protocolVersion)
    { }

    TInvokeResult Invoke()
    {
        auto context = CreateClientContext();
        auto response = NYT::New<TResponse>(std::move(context));
        auto future = response->GetAsyncResult();
        DoInvoke(std::move(response));
        return future;
    }

    // Override base methods for fluent use.
    TThisPtr SetTimeout(TNullable<TDuration> timeout)
    {
        TClientRequest::SetTimeout(timeout);
        return this;
    }

    TThisPtr SetRequestAck(bool value)
    {
        TClientRequest::SetRequestAck(value);
        return this;
    }

    TThisPtr SetCodec(NCompression::ECodec codec)
    {
        Codec_ = codec;
        return this;
    }

    TThisPtr SetRequestHeavy(bool value)
    {
        TClientRequest::SetRequestHeavy(value);
        return this;
    }

    TThisPtr SetResponseHeavy(bool value)
    {
        TClientRequest::SetResponseHeavy(value);
        return this;
    }

private:
    NCompression::ECodec Codec_ = NCompression::ECodec::None;

    virtual TSharedRef SerializeBody() const override
    {
        TSharedRef data;
        YCHECK(SerializeToProtoWithEnvelope(*this, &data, Codec_));
        return data;
    }

};

////////////////////////////////////////////////////////////////////////////////

//! Handles response for an RPC request.
struct IClientResponseHandler
    : public virtual TRefCounted
{
    //! Called when request delivery is acknowledged.
    virtual void OnAcknowledgement() = 0;

    //! Called if the request is replied with #EErrorCode::OK.
    /*!
     *  \param message A message containing the response.
     */
    virtual void OnResponse(TSharedRefArray message) = 0;

    //! Called if the request fails.
    /*!
     *  \param error An error that has occurred.
     */
    virtual void OnError(const TError& error) = 0;

};

DEFINE_REFCOUNTED_TYPE(IClientResponseHandler)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClientResponseState,
    (Sent)
    (Ack)
    (Done)
);

//! Provides a common base for both one-way and two-way responses.
class TClientResponseBase
    : public IClientResponseHandler
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

protected:
    using EState = EClientResponseState;

    TSpinLock SpinLock_;
    EState State_;

    TClientContextPtr ClientContext_;

    explicit TClientResponseBase(TClientContextPtr clientContext);

    virtual void FireCompleted(const TError& error) = 0;

    // IClientResponseHandler implementation.
    virtual void OnError(const TError& error) override;

    void BeforeCompleted();
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a two-way response.
class TClientResponse
    : public TClientResponseBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

public:
    TSharedRefArray GetResponseMessage() const;

protected:
    explicit TClientResponse(TClientContextPtr clientContext);

    virtual void DeserializeBody(const TRef& data) = 0;

private:
    // Protected by #SpinLock.
    TSharedRefArray ResponseMessage_;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement() override;
    virtual void OnResponse(TSharedRefArray message) override;

    void Deserialize(TSharedRefArray responseMessage);

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    typedef TIntrusivePtr<TTypedClientResponse> TThisPtr;

    explicit TTypedClientResponse(TClientContextPtr clientContext)
        : TClientResponse(std::move(clientContext))
    { }

    TFuture<TThisPtr> GetAsyncResult()
    {
        return Promise_;
    }

private:
    TPromise<TThisPtr> Promise_ = NewPromise<TThisPtr>();


    virtual void FireCompleted(const TError& error) override
    {
        BeforeCompleted();
        if (error.IsOK()) {
            Promise_.Set(this);
        } else {
            Promise_.Set(error);
        }
        Promise_.Reset();
    }

    virtual void DeserializeBody(const TRef& data) override
    {
        YCHECK(DeserializeFromProtoWithEnvelope(this, data));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a one-way response.
class TOneWayClientResponse
    : public TClientResponseBase
{
public:
    explicit TOneWayClientResponse(TClientContextPtr clientContext);

    TFuture<void> GetAsyncResult();

private:
    TPromise<void> Promise_;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement() override;
    virtual void OnResponse(TSharedRefArray message) override;

    virtual void FireCompleted(const TError& error);

};

DEFINE_REFCOUNTED_TYPE(TOneWayClientResponse)

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TRsp##method> TRsp##method; \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method> TReq##method; \
    typedef ::NYT::TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef ::NYT::TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    typedef ::NYT::TErrorOr<TRsp##method##Ptr> TErrorOrRsp##method##Ptr; \
    \
    TReq##method##Ptr method() \
    { \
        static Stroka MethodName(#method); \
        return ::NYT::New<TReq##method>(Channel_, ServiceName_, MethodName, false, ProtocolVersion_) \
            ->SetTimeout(DefaultTimeout_) \
            ->SetRequestAck(DefaultRequestAck_); \
    }

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ONE_WAY_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, ::NYT::NRpc::TOneWayClientResponse> TReq##method; \
    typedef ::NYT::TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    \
    TReq##method##Ptr method() \
    { \
        static Stroka MethodName(#method); \
        return ::NYT::New<TReq##method>(Channel_, ServiceName_, MethodName, true, ProtocolVersion_) \
            ->SetTimeout(DefaultTimeout_) \
            ->SetRequestAck(DefaultRequestAck_); \
    }

////////////////////////////////////////////////////////////////////////////////

class TProxyBase
{
public:
    static const int DefaultProtocolVersion = 0;
    static const int GenericProtocolVersion = -1;

    DEFINE_RPC_PROXY_METHOD(NProto, Discover);

protected:
    TProxyBase(
        IChannelPtr channel,
        const Stroka& serviceName,
        int protocolVersion = DefaultProtocolVersion);

    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, DefaultTimeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, DefaultRequestAck);

    Stroka ServiceName_;
    IChannelPtr Channel_;
    int ProtocolVersion_;

};

////////////////////////////////////////////////////////////////////////////////

class TGenericProxy
    : public TProxyBase
{
public:
    TGenericProxy(
        IChannelPtr channel,
        const Stroka& serviceName);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
