#pragma once

#include "public.h"
#include "channel.h"

#include <yt/core/actions/future.h>

#include <yt/core/bus/client.h>

#include <yt/core/compression/codec.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/tracing/trace_context.h>

#include <yt/core/concurrency/public.h>

#include <atomic>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest
    : public virtual TRefCounted
{
    virtual TSharedRefArray Serialize() = 0;

    virtual const NProto::TRequestHeader& Header() const = 0;
    virtual NProto::TRequestHeader& Header() = 0;

    virtual bool IsStreamingEnabled() const = 0;

    virtual const TStreamingParameters& ClientAttachmentsStreamingParameters() const = 0;
    virtual TStreamingParameters& ClientAttachmentsStreamingParameters() = 0;

    virtual const TStreamingParameters& ServerAttachmentsStreamingParameters() const = 0;
    virtual TStreamingParameters& ServerAttachmentsStreamingParameters() = 0;

    virtual NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const = 0;
    virtual NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const = 0;

    virtual bool IsHeavy() const = 0;

    virtual TRequestId GetRequestId() const = 0;
    virtual TRealmId GetRealmId() const = 0;
    virtual const TString& GetService() const = 0;
    virtual const TString& GetMethod() const = 0;

    virtual const TString& GetUser() const = 0;
    virtual void SetUser(const TString& user) = 0;

    virtual void SetUserAgent(const TString& userAgent) = 0;

    virtual bool GetRetry() const = 0;
    virtual void SetRetry(bool value) = 0;

    virtual TMutationId GetMutationId() const = 0;
    virtual void SetMutationId(TMutationId id) = 0;

    virtual EMultiplexingBand GetMultiplexingBand() const = 0;
    virtual void SetMultiplexingBand(EMultiplexingBand band) = 0;

    virtual size_t GetHash() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientRequest)

////////////////////////////////////////////////////////////////////////////////

class TClientContext
    : public TIntrinsicRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TRequestId, RequestId);
    DEFINE_BYVAL_RO_PROPERTY(NTracing::TTraceContextPtr, TraceContext);
    DEFINE_BYVAL_RO_PROPERTY(TString, Service);
    DEFINE_BYVAL_RO_PROPERTY(TString, Method);
    DEFINE_BYVAL_RO_PROPERTY(bool, Heavy);
    DEFINE_BYVAL_RO_PROPERTY(NYTAlloc::EMemoryZone, MemoryZone);
    DEFINE_BYVAL_RO_PROPERTY(TAttachmentsOutputStreamPtr, RequestAttachmentsStream);
    DEFINE_BYVAL_RO_PROPERTY(TAttachmentsInputStreamPtr, ResponseAttachmentsStream);
    DEFINE_BYVAL_RO_PROPERTY(NYTAlloc::TMemoryTag, ResponseMemoryTag);

public:
    TClientContext(
        TRequestId requestId,
        NTracing::TTraceContextPtr traceContext,
        const TString& service,
        const TString& method,
        bool heavy,
        NYTAlloc::EMemoryZone memoryZone,
        TAttachmentsOutputStreamPtr requestAttachmentsStream,
        TAttachmentsInputStreamPtr responseAttachmentsStream,
        NYTAlloc::TMemoryTag responseMemoryTag);
};

DEFINE_REFCOUNTED_TYPE(TClientContext)

////////////////////////////////////////////////////////////////////////////////

class TClientRequest
    : public IClientRequest
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, AcknowledgementTimeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, Heavy, false);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, RequestCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, ResponseCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(bool, EnableLegacyRpcCodecs, true);
    DEFINE_BYVAL_RW_PROPERTY(bool, GenerateAttachmentChecksums, true);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYTAlloc::TMemoryTag>, ResponseMemoryTag);
    // For testing purposes only.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, SendDelay);
    DEFINE_BYVAL_RW_PROPERTY(NYTAlloc::EMemoryZone, MemoryZone, NYTAlloc::EMemoryZone::Normal);

public:
    virtual TSharedRefArray Serialize() override;

    virtual NProto::TRequestHeader& Header() override;
    virtual const NProto::TRequestHeader& Header() const override;

    virtual bool IsStreamingEnabled() const override;

    virtual const TStreamingParameters& ClientAttachmentsStreamingParameters() const override;
    virtual TStreamingParameters& ClientAttachmentsStreamingParameters() override;

    virtual const TStreamingParameters& ServerAttachmentsStreamingParameters() const override;
    virtual TStreamingParameters& ServerAttachmentsStreamingParameters() override;

    virtual NConcurrency::IAsyncZeroCopyOutputStreamPtr GetRequestAttachmentsStream() const override;
    virtual NConcurrency::IAsyncZeroCopyInputStreamPtr GetResponseAttachmentsStream() const override;

    virtual TRequestId GetRequestId() const override;
    virtual TRealmId GetRealmId() const override;
    virtual const TString& GetService() const override;
    virtual const TString& GetMethod() const override;

    virtual const TString& GetUser() const override;
    virtual void SetUser(const TString& user) override;

    virtual void SetUserAgent(const TString& userAgent) override;

    virtual bool GetRetry() const override;
    virtual void SetRetry(bool value) override;

    virtual TMutationId GetMutationId() const override;
    virtual void SetMutationId(TMutationId id) override;

    virtual size_t GetHash() const override;

    virtual EMultiplexingBand GetMultiplexingBand() const override;
    virtual void SetMultiplexingBand(EMultiplexingBand band) override;

protected:
    const IChannelPtr Channel_;
    const bool StreamingEnabled_;

    mutable NProto::TRequestHeader Header_;
    mutable TSharedRefArray SerializedData_;
    mutable std::optional<size_t> Hash_;

    EMultiplexingBand MultiplexingBand_ = EMultiplexingBand::Default;
    bool FirstTimeSerialization_ = true;

    TStreamingParameters ClientAttachmentsStreamingParameters_;
    TStreamingParameters ServerAttachmentsStreamingParameters_;

    TAttachmentsOutputStreamPtr RequestAttachmentsStream_;
    TAttachmentsInputStreamPtr ResponseAttachmentsStream_;


    TClientRequest(
        IChannelPtr channel,
        const TServiceDescriptor& serviceDescriptor,
        const TMethodDescriptor& methodDescriptor);

    // NB: doesn't copy base class.
    TClientRequest(const TClientRequest& other);

    virtual bool IsHeavy() const override;

    virtual TSharedRefArray SerializeData() const = 0;

    TClientContextPtr CreateClientContext();

    IClientRequestControlPtr Send(IClientResponseHandlerPtr responseHandler);

private:
    void OnPullRequestAttachmentsStream();
    void OnRequestStreamingPayloadAcked(int sequenceNumber, const TError& error);
    void OnResponseAttachmentsStreamRead();
    void OnResponseStreamingFeedbackAcked(const TError& error);

    const IInvokerPtr& GetInvoker() const;

    void TraceRequest(const NTracing::TTraceContextPtr& traceContext);

    void SetCodecsInHeader();
    void SetStreamingParametersInHeader();

    const TSharedRefArray& GetSerializedData() const;

    TWeakPtr<IClientRequestControl> RequestControl_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
class TTypedClientRequest
    : public TClientRequest
    , public TRequestMessage
{
public:
    using TThisPtr = TIntrusivePtr<TTypedClientRequest>;

    TTypedClientRequest(
        IChannelPtr channel,
        const TServiceDescriptor& serviceDescriptor,
        const TMethodDescriptor& methodDescriptor);

    TFuture<typename TResponse::TResult> Invoke();

private:
    virtual TSharedRefArray SerializeData() const override;
};

////////////////////////////////////////////////////////////////////////////////

//! Handles the outcome of a single RPC request.
struct IClientResponseHandler
    : public virtual TRefCounted
{
    //! Called when request delivery is acknowledged.
    virtual void HandleAcknowledgement() = 0;

    //! Called if the request is replied with #EErrorCode::OK.
    /*!
     *  \param message A message containing the response.
     */
    virtual void HandleResponse(TSharedRefArray message) = 0;

    //! Called if the request fails.
    /*!
     *  \param error An error that has occurred.
     */
    virtual void HandleError(const TError& error) = 0;

    //! Enables passing streaming data from the service to clients.
    virtual void HandleStreamingPayload(const TStreamingPayload& payload) = 0;

    //! Enables the service to notify clients about its progress in receiving streaming data.
    virtual void HandleStreamingFeedback(const TStreamingFeedback& feedback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClientResponseHandler)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClientResponseState,
    (Sent)
    (Ack)
    (Done)
);

//! Provides a common base for both one-way and two-way responses.
class TClientResponse
    : public IClientResponseHandler
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    const NProto::TResponseHeader& Header() const;

    TSharedRefArray GetResponseMessage() const;

    //! Returns total size: response message size plus attachments.
    size_t GetTotalSize() const;

protected:
    const TClientContextPtr ClientContext_;

    using EState = EClientResponseState;
    std::atomic<EState> State_ = {EState::Sent};


    explicit TClientResponse(TClientContextPtr clientContext);

    virtual void DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = std::nullopt) = 0;

    // IClientResponseHandler implementation.
    virtual void HandleError(const TError& error) override;
    virtual void HandleAcknowledgement() override;
    virtual void HandleResponse(TSharedRefArray message) override;
    virtual void HandleStreamingPayload(const TStreamingPayload& payload) override;
    virtual void HandleStreamingFeedback(const TStreamingFeedback& feedback) override;

    void Finish(const TError& error);

    virtual void SetPromise(const TError& error) = 0;

    const IInvokerPtr& GetInvoker();

private:
    NProto::TResponseHeader Header_;
    TSharedRefArray ResponseMessage_;

    void TraceResponse();
    void DoHandleError(const TError& error);

    void DoHandleResponse(TSharedRefArray message);
    void Deserialize(TSharedRefArray responseMessage);
};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedClientResponse
    : public TClientResponse
    , public TResponseMessage
{
public:
    using TResult = TIntrusivePtr<TTypedClientResponse>;

    explicit TTypedClientResponse(TClientContextPtr clientContext);

    TPromise<TResult> GetPromise();

private:
    TPromise<TResult> Promise_ = NewPromise<TResult>();


    virtual void SetPromise(const TError& error) override;
    virtual void DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId = std::nullopt) override;
};

////////////////////////////////////////////////////////////////////////////////

struct TServiceDescriptor
{
    TString ServiceName;
    TString Namespace;
    TProtocolVersion ProtocolVersion = DefaultProtocolVersion;

    explicit TServiceDescriptor(const TString& serviceName);

    TServiceDescriptor& SetProtocolVersion(int majorVersion);
    TServiceDescriptor& SetProtocolVersion(TProtocolVersion version);
    TServiceDescriptor& SetNamespace(const TString& value);

    TString GetFullServiceName() const;
};

#define DEFINE_RPC_PROXY(type, name, ...) \
    static const ::NYT::NRpc::TServiceDescriptor& GetDescriptor() \
    { \
        static const auto Descriptor = ::NYT::NRpc::TServiceDescriptor(#name) __VA_ARGS__; \
        return Descriptor; \
    } \
    \
    explicit type(::NYT::NRpc::IChannelPtr channel) \
        : ::NYT::NRpc::TProxyBase(std::move(channel), GetDescriptor()) \
    { }

////////////////////////////////////////////////////////////////////////////////

struct TMethodDescriptor
{
    TString MethodName;
    EMultiplexingBand MultiplexingBand = EMultiplexingBand::Default;
    bool StreamingEnabled = false;

    explicit TMethodDescriptor(const TString& methodName);

    TMethodDescriptor& SetMultiplexingBand(EMultiplexingBand value);
    TMethodDescriptor& SetStreamingEnabled(bool value);
};

#define DEFINE_RPC_PROXY_METHOD(ns, method, ...) \
    using TRsp##method = ::NYT::NRpc::TTypedClientResponse<ns::TRsp##method>; \
    using TReq##method = ::NYT::NRpc::TTypedClientRequest<ns::TReq##method, TRsp##method>; \
    using TRsp##method##Ptr = ::NYT::TIntrusivePtr<TRsp##method>; \
    using TReq##method##Ptr = ::NYT::TIntrusivePtr<TReq##method>; \
    using TErrorOrRsp##method##Ptr = ::NYT::TErrorOr<TRsp##method##Ptr>; \
    \
    TReq##method##Ptr method() \
    { \
        static const auto Descriptor = ::NYT::NRpc::TMethodDescriptor(#method) __VA_ARGS__; \
        return CreateRequest<TReq##method>(Descriptor); \
    }

////////////////////////////////////////////////////////////////////////////////

class TProxyBase
{
public:
    DEFINE_RPC_PROXY_METHOD(NProto, Discover);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, DefaultTimeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, DefaultAcknowledgementTimeout);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, DefaultRequestCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(NCompression::ECodec, DefaultResponseCodec, NCompression::ECodec::None);
    DEFINE_BYVAL_RW_PROPERTY(bool, DefaultEnableLegacyRpcCodecs, true);

    DEFINE_BYREF_RW_PROPERTY(TStreamingParameters, DefaultClientAttachmentsStreamingParameters);
    DEFINE_BYREF_RW_PROPERTY(TStreamingParameters, DefaultServerAttachmentsStreamingParameters);

protected:
    const IChannelPtr Channel_;
    const TServiceDescriptor ServiceDescriptor_;

    TProxyBase(
        IChannelPtr channel,
        const TServiceDescriptor& descriptor);

    template <class T>
    TIntrusivePtr<T> CreateRequest(const TMethodDescriptor& methodDescriptor);
};

////////////////////////////////////////////////////////////////////////////////

class TGenericProxy
    : public TProxyBase
{
public:
    TGenericProxy(
        IChannelPtr channel,
        const TServiceDescriptor& descriptor);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define CLIENT_INL_H_
#include "client-inl.h"
#undef CLIENT_INL_H_
