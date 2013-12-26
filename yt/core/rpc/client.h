#pragma once

#include "public.h"
#include "channel.h"

#include <core/misc/property.h>
#include <core/concurrency/delayed_executor.h>
#include <core/misc/metric.h>
#include <core/misc/protobuf_helpers.h>

#include <core/compression/public.h>

#include <core/bus/client.h>

#include <core/rpc/rpc.pb.h>

#include <core/actions/future.h>

#include <core/logging/log.h>

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

    TProxyBase(IChannelPtr channel, const Stroka& serviceName);

    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, DefaultTimeout);

    Stroka ServiceName;
    IChannelPtr Channel;

};

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest
    : public virtual TRefCounted
{
    virtual TSharedRefArray Serialize() const = 0;

    virtual const NProto::TRequestHeader& Header() const = 0;
    virtual NProto::TRequestHeader& Header() = 0;

    virtual bool IsOneWay() const = 0;
    virtual bool IsRequestHeavy() const = 0;
    virtual bool IsResponseHeavy() const = 0;

    virtual TRequestId GetRequestId() const = 0;

    virtual const Stroka& GetService() const = 0;
    virtual const Stroka& GetVerb() const = 0;

    virtual TInstant GetStartTime() const = 0;
    virtual void SetStartTime(TInstant value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
    DEFINE_BYREF_RW_PROPERTY(NProto::TRequestHeader, Header);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, RequestHeavy);
    DEFINE_BYVAL_RW_PROPERTY(bool, ResponseHeavy);

public:
    virtual TSharedRefArray Serialize() const override;

    virtual bool IsOneWay() const override;
    
    virtual TRequestId GetRequestId() const override;

    virtual const Stroka& GetService() const override;
    virtual const Stroka& GetVerb() const override;

    virtual TInstant GetStartTime() const override;
    virtual void SetStartTime(TInstant value) override;

protected:
    IChannelPtr Channel;

    TClientRequest(
        IChannelPtr channel,
        const Stroka& service,
        const Stroka& verb,
        bool oneWay);

    virtual bool IsRequestHeavy() const;
    virtual bool IsResponseHeavy() const;
    virtual TSharedRef SerializeBody() const = 0;

    void DoInvoke(IClientResponseHandlerPtr responseHandler);
};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern NLog::TLogger RpcClientLogger;

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
        , Codec(NCompression::ECodec::None)
    { }

    TFuture< TIntrusivePtr<TResponse> > Invoke()
    {
        auto response = NYT::New<TResponse>(GetRequestId());
        auto promise = response->GetAsyncResult();
        DoInvoke(response);
        return promise;
    }

    // Override base methods for fluent use.
    TIntrusivePtr<TTypedClientRequest> SetTimeout(TNullable<TDuration> timeout)
    {
        TClientRequest::SetTimeout(timeout);
        return this;
    }

    TIntrusivePtr<TTypedClientRequest> SetCodec(NCompression::ECodec codec)
    {
        Codec = codec;
        return this;
    }

    TIntrusivePtr<TTypedClientRequest> SetRequestHeavy(bool value)
    {
        TClientRequest::SetRequestHeavy(value);
        return this;
    }

    TIntrusivePtr<TTypedClientRequest> SetResponseHeavy(bool value)
    {
        TClientRequest::SetResponseHeavy(value);
        return this;
    }

private:
    NCompression::ECodec Codec;

    virtual TSharedRef SerializeBody() const override
    {
        TSharedRef data;
        YCHECK(SerializeToProtoWithEnvelope(*this, &data, Codec));
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

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for both one-way and two-way responses.
class TClientResponseBase
    : public IClientResponseHandler
{
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(TError, Error);
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);

public:
    bool IsOK() const;
    operator TError();

protected:
    DECLARE_ENUM(EState,
        (Sent)
        (Ack)
        (Done)
    );

    TSpinLock SpinLock; // Protects state.
    EState State;

    explicit TClientResponseBase(const TRequestId& requestId);

    virtual void FireCompleted() = 0;

    // IClientResponseHandler implementation.
    virtual void OnError(const TError& error) override;


};

////////////////////////////////////////////////////////////////////////////////

//! Describes a two-way response.
class TClientResponse
    : public TClientResponseBase
{
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

public:
    TSharedRefArray GetResponseMessage() const;

protected:
    explicit TClientResponse(const TRequestId& requestId);

    virtual void DeserializeBody(const TRef& data) = 0;

private:
    // Protected by #SpinLock.
    TSharedRefArray ResponseMessage;

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

    explicit TTypedClientResponse(const TRequestId& requestId)
        : TClientResponse(requestId)
        , Promise(NewPromise<TThisPtr>())
    { }

    TFuture<TThisPtr> GetAsyncResult()
    {
        return Promise;
    }

private:
    TPromise<TThisPtr> Promise;

    virtual void FireCompleted()
    {
        Promise.Set(this);
        Promise.Reset();
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
    typedef TIntrusivePtr<TOneWayClientResponse> TThisPtr;

    explicit TOneWayClientResponse(const TRequestId& requestId);

    TFuture<TThisPtr> GetAsyncResult();

private:
    TPromise<TThisPtr> Promise;

    // IClientResponseHandler implementation.
    virtual void OnAcknowledgement() override;
    virtual void OnResponse(TSharedRefArray message) override;

    virtual void FireCompleted();

};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedClientResponse<ns::TRsp##method> TRsp##method; \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method> TReq##method; \
    \
    typedef ::NYT::TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef ::NYT::TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    \
    typedef ::NYT::TFuture< TRsp##method##Ptr > TInv##method; \
    \
    TReq##method##Ptr method() \
    { \
        return \
            ::NYT::New<TReq##method>(Channel, ServiceName, #method, false) \
            ->SetTimeout(DefaultTimeout_); \
    }

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ONE_WAY_RPC_PROXY_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayClientResponse TRsp##method; \
    typedef ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method> TReq##method; \
    \
    typedef ::NYT::TIntrusivePtr<TRsp##method> TRsp##method##Ptr; \
    typedef ::NYT::TIntrusivePtr<TReq##method> TReq##method##Ptr; \
    \
    typedef ::NYT::TFuture< TRsp##method##Ptr > TInv##method; \
    \
    TReq##method##Ptr method() \
    { \
        return \
            ::NYT::New<TReq##method>(Channel, ServiceName, #method, true) \
            ->SetTimeout(DefaultTimeout_); \
    }

////////////////////////////////////////////////////////////////////////////////
} // namespace NRpc
} // namespace NYT
