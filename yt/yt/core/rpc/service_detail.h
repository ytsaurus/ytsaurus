#pragma once

#include "public.h"

#include "client.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "service.h"
#include "config.h"

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/misc/object_pool.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/ref.h>

#include <yt/yt/core/profiling/profiler.h>

#include <yt/yt/core/rpc/message_format.h>

#include <yt/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/syncmap/map.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage>
class TTypedServiceContextBase;

template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext;

template <class TRequestMessage>
class TTypedServiceRequest
    : public TRequestMessage
{
public:
    using TMessage = TRequestMessage;

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    NConcurrency::IAsyncZeroCopyInputStreamPtr GetAttachmentsStream()
    {
        return Context_->GetRequestAttachmentsStream();
    }

    void Reset()
    {
        TRequestMessage::Clear();
        Attachments_.clear();
        Context_ = nullptr;
    }

private:
    template <class TRequestMessage_, class TResponseMessage_>
    friend class TTypedServiceContext;

    IServiceContext* Context_ = nullptr;
};

template <class TRequestMessage>
struct TPooledTypedRequestTraits
    : public TPooledObjectTraitsBase<TTypedServiceRequest<TRequestMessage>>
{
    static void Clean(TTypedServiceRequest<TRequestMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    using TMessage = TResponseMessage;

    DEFINE_BYREF_RW_PROPERTY(std::vector<TSharedRef>, Attachments);

    NConcurrency::IAsyncZeroCopyOutputStreamPtr GetAttachmentsStream()
    {
        return Context_->GetResponseAttachmentsStream();
    }

private:
    template <class TRequestMessage_, class TResponseMessage_>
    friend class TTypedServiceContext;

    IServiceContext* Context_ = nullptr;
};

template <class TResponseMessage>
struct TPooledTypedResponseTraits
    : public TPooledObjectTraitsBase<TTypedServiceResponse<TResponseMessage>>
{
    static void Clean(TTypedServiceResponse<TResponseMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes request handling options.
struct THandlerInvocationOptions
{
    //! Should we be deserializing the request and serializing the request
    //! in a separate thread?
    bool Heavy = false;

    //! In case the client has provided "none" response codec, this value is used instead.
    NCompression::ECodec ResponseCodec = NCompression::ECodec::None;

    THandlerInvocationOptions& SetHeavy(bool value)
    {
        Heavy = value;
        return *this;
    }

    THandlerInvocationOptions& SetResponseCodec(NCompression::ECodec value)
    {
        ResponseCodec = value;
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext
    : public TServiceContextWrapper
{
public:
    using TTypedRequest = TTypedServiceRequest<TRequestMessage>;
    using TTypedResponse = TTypedServiceResponse<TResponseMessage>;
    using TThis = TTypedServiceContext<TRequestMessage, TResponseMessage>;

    TTypedServiceContext(
        IServiceContextPtr context,
        const THandlerInvocationOptions& options)
        : TServiceContextWrapper(std::move(context))
        , Options_(options)
    {
        Response_ = UnderlyingContext_->IsPooled()
            ? ObjectPool<TTypedResponse, TPooledTypedResponseTraits<TResponseMessage>>().Allocate()
            : std::make_shared<TTypedResponse>();

        Response_->Context_ = this->UnderlyingContext_.Get();

        if (GetResponseCodec() == NCompression::ECodec::None) {
            SetResponseCodec(Options_.ResponseCodec);
        }
    }

    bool DeserializeRequest()
    {
        if (UnderlyingContext_->IsPooled()) {
            Request_ = ObjectPool<TTypedRequest, TPooledTypedRequestTraits<TRequestMessage>>().Allocate();
        } else {
            Request_ = std::make_shared<TTypedRequest>();
        }
        Request_->Context_ = UnderlyingContext_.Get();

        const auto& requestHeader = GetRequestHeader();
        auto body = UnderlyingContext_->GetRequestBody();
        if (requestHeader.has_request_format()) {
            auto format = static_cast<EMessageFormat>(GetRequestHeader().request_format());

            NYson::TYsonString formatOptionsYson;
            if (GetRequestHeader().has_request_format_options()) {
                formatOptionsYson = NYson::TYsonString(GetRequestHeader().request_format_options());
            }
            if (format != EMessageFormat::Protobuf) {
                body = ConvertMessageFromFormat(
                    body,
                    format,
                    NYson::ReflectProtobufMessageType<TRequestMessage>(),
                    formatOptionsYson);
            }
        }

        // COMPAT(kiselyovp): legacy RPC codecs
        std::optional<NCompression::ECodec> bodyCodecId;
        NCompression::ECodec attachmentCodecId;
        if (requestHeader.has_request_codec()) {
            int intCodecId = requestHeader.request_codec();
            NCompression::ECodec codecId;
            if (!TryEnumCast(intCodecId, &codecId)) {
                UnderlyingContext_->Reply(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Request codec %v is not supported",
                    intCodecId));
                return false;
            }
            bodyCodecId = codecId;
            attachmentCodecId = codecId;
        } else {
            bodyCodecId = std::nullopt;
            attachmentCodecId = NCompression::ECodec::None;
        }

        bool deserializationSucceeded = bodyCodecId
            ? TryDeserializeProtoWithCompression(Request_.get(), body, *bodyCodecId)
            : TryDeserializeProtoWithEnvelope(Request_.get(), body);
        if (!deserializationSucceeded) {
            UnderlyingContext_->Reply(TError(
                NRpc::EErrorCode::ProtocolError,
                "Error deserializing request body"));
            return false;
        }

        std::vector<TSharedRef> requestAttachments;
        try {
            requestAttachments = DecompressAttachments(
                UnderlyingContext_->RequestAttachments(),
                attachmentCodecId);
        } catch (const std::exception& ex) {
            UnderlyingContext_->Reply(TError(
                NRpc::EErrorCode::ProtocolError,
                "Error deserializing request attachments")
                << ex);
            return false;
        }

        Request_->Attachments() = std::move(requestAttachments);

        return true;
    }

    const TTypedRequest& Request() const
    {
        return *Request_;
    }

    TTypedRequest& Request()
    {
        return *Request_;
    }

    const TTypedResponse& Response() const
    {
        return *Response_;
    }

    TTypedResponse& Response()
    {
        return *Response_;
    }


    using IServiceContext::Reply;

    void Reply()
    {
        Reply(TError());
    }

    virtual void Reply(const TError& error) override
    {
        if (this->Options_.Heavy) {
            TDispatcher::Get()->GetHeavyInvoker()->Invoke(BIND(
                &TThis::DoReply,
                MakeStrong(this),
                error));
        } else {
            this->DoReply(error);
        }
    }

protected:
    const THandlerInvocationOptions Options_;
    typename TObjectPool<TTypedRequest, TPooledTypedRequestTraits<TRequestMessage>>::TObjectPtr Request_;
    typename TObjectPool<TTypedResponse, TPooledTypedResponseTraits<TResponseMessage>>::TObjectPtr Response_;

    void DoReply(const TError& error)
    {
        if (error.IsOK()) {
            const auto& requestHeader = GetRequestHeader();

            // COMPAT(kiselyovp): legacy RPC codecs
            bool enableBodyEnvelope;
            NCompression::ECodec attachmentCodecId;
            auto bodyCodecId = UnderlyingContext_->GetResponseCodec();
            if (requestHeader.has_request_codec()) {
                enableBodyEnvelope = false;
                attachmentCodecId = bodyCodecId;
            } else {
                enableBodyEnvelope = true;
                attachmentCodecId = NCompression::ECodec::None;
            }

            auto serializedBody = enableBodyEnvelope
                ? SerializeProtoToRefWithEnvelope(*Response_, bodyCodecId)
                : SerializeProtoToRefWithCompression(*Response_, bodyCodecId, false);

            if (requestHeader.has_response_format()) {
                int intFormat = requestHeader.response_format();
                EMessageFormat format;
                if (!TryEnumCast(intFormat, &format)) {
                    UnderlyingContext_->Reply(TError(
                        NRpc::EErrorCode::ProtocolError,
                        "Message format %v is not supported",
                        intFormat));
                    return;
                }

                NYson::TYsonString formatOptionsYson;
                if (requestHeader.has_response_format_options()) {
                    formatOptionsYson = NYson::TYsonString(requestHeader.response_format_options());
                }

                if (format != EMessageFormat::Protobuf) {
                    serializedBody = ConvertMessageToFormat(
                        serializedBody,
                        format,
                        NYson::ReflectProtobufMessageType<TResponseMessage>(),
                        formatOptionsYson);
                }
            }

            auto responseAttachments = CompressAttachments(Response_->Attachments(), attachmentCodecId);

            this->UnderlyingContext_->SetResponseBody(std::move(serializedBody));
            this->UnderlyingContext_->ResponseAttachments() = std::move(responseAttachments);
        }
        this->UnderlyingContext_->Reply(error);
        if (UnderlyingContext_->IsPooled()) {
            this->Request_.reset();
            this->Response_.reset();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    using TCtx##method = ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method>; \
    using TCtx##method##Ptr = ::NYT::TIntrusivePtr<TCtx##method>; \
    using TReq##method = TCtx##method::TTypedRequest; \
    using TRsp##method = TCtx##method::TTypedResponse; \
    \
    void method##LiteThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) { \
            return; \
        } \
        auto* request = &typedContext->Request(); \
        auto* response = &typedContext->Response(); \
        this->method(request, response, typedContext); \
    } \
    \
    ::NYT::NRpc::TServiceBase::TLiteHandler method##HeavyThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) { \
            return ::NYT::NRpc::TServiceBase::TLiteHandler(); \
        } \
        return \
            BIND([=] ( \
                const ::NYT::NRpc::IServiceContextPtr&, \
                const ::NYT::NRpc::THandlerInvocationOptions&) \
            { \
                auto* request = &typedContext->Request(); \
                auto* response = &typedContext->Response(); \
                this->method(request, response, typedContext); \
            }); \
    }

#define DECLARE_RPC_SERVICE_METHOD(ns, method) \
    DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    \
    void method( \
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

#define DEFINE_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        const TCtx##method##Ptr& context)

#define RPC_SERVICE_METHOD_DESC(method) \
    ::NYT::NRpc::TServiceBase::TMethodDescriptor( \
        #method, \
        BIND(&std::remove_reference<decltype(*this)>::type::method##LiteThunk, ::NYT::Unretained(this)), \
        BIND(&std::remove_reference<decltype(*this)>::type::method##HeavyThunk, ::NYT::Unretained(this)))

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public virtual IService
{
public:
    virtual void Configure(
        const TServiceCommonConfigPtr& configDefaults,
        const NYTree::INodePtr& configNode) override;
    virtual TFuture<void> Stop() override;

    virtual const TServiceId& GetServiceId() const override;

    virtual void HandleRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        NYT::NBus::IBusPtr replyBus) override;
    virtual void HandleRequestCancelation(TRequestId requestId) override;
    virtual void HandleStreamingPayload(
        TRequestId requestId,
        const TStreamingPayload& payload) override;
    virtual void HandleStreamingFeedback(
        TRequestId requestId,
        const TStreamingFeedback& feedback) override;

protected:
    using TLiteHandler = TCallback<void(const IServiceContextPtr&, const THandlerInvocationOptions&)>;
    using THeavyHandler = TCallback<TLiteHandler(const IServiceContextPtr&, const THandlerInvocationOptions&)>;

    class TServiceContext;
    using TServiceContextPtr = TIntrusivePtr<TServiceContext>;

    using TInvokerProvider = TCallback<IInvokerPtr(const IServiceContextPtr&)>;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        // Defaults.
        TMethodDescriptor(
            TString method,
            TLiteHandler liteHandler,
            THeavyHandler heavyHandler);

        //! Invoker used for executing the handler.
        //! Overrides the default one (unless null).
        IInvokerPtr Invoker;

        //! Invoker provider is called upon receiving a request to determine
        //! the actual invoker to be used for handling the request.
        //! Overrides #Invoker (unless null).
        TInvokerProvider InvokerProvider;

        //! Service method name.
        TString Method;

        //! A handler that will serve lite requests.
        TLiteHandler LiteHandler;

        //! A handler that will serve heavy requests.
        THeavyHandler HeavyHandler;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

        //! Maximum number of requests in queue (both waiting and executing).
        int QueueSizeLimit = 10'000;

        //! Maximum number of requests executing concurrently.
        int ConcurrencyLimit = 10'000;

        //! System requests are completely transparent to derived classes;
        //! in particular, |BeforeInvoke| is not called.
        //! Also system methods do not require authentication.
        bool System = false;

        //! Log level for events emitted via |Set(Request|Response)Info|-like functions.
        NLogging::ELogLevel LogLevel = NLogging::ELogLevel::Debug;

        //! Logging suppression timeout for this method requests.
        TDuration LoggingSuppressionTimeout = TDuration::Zero();

        //! Cancelable requests can be canceled by clients.
        //! This, however, requires additional book-keeping at server-side so one is advised
        //! to only mark cancelable those methods taking a considerable time to complete.
        bool Cancelable = false;

        //! If |true| then Bus is expected to be generating checksums for the whole response content,
        //! including attachments (unless the connection is local or the checksums are explicitly disabled).
        //! If |false| then Bus will only be generating such checksums for RPC header and response body
        //! but not attachments.
        bool GenerateAttachmentChecksums = true;

        //! If |true| then the method supports attachments streaming.
        bool StreamingEnabled = false;

        //! If |true| then requests and responses are pooled.
        bool Pooled = true;

        TMethodDescriptor& SetInvoker(IInvokerPtr value);
        TMethodDescriptor& SetInvokerProvider(TInvokerProvider value);
        TMethodDescriptor& SetHeavy(bool value);
        TMethodDescriptor& SetResponseCodec(NCompression::ECodec value);
        TMethodDescriptor& SetQueueSizeLimit(int value);
        TMethodDescriptor& SetConcurrencyLimit(int value);
        TMethodDescriptor& SetSystem(bool value);
        TMethodDescriptor& SetLogLevel(NLogging::ELogLevel value);
        TMethodDescriptor& SetLoggingSuppressionTimeout(TDuration timeout);
        TMethodDescriptor& SetCancelable(bool value);
        TMethodDescriptor& SetGenerateAttachmentChecksums(bool value);
        TMethodDescriptor& SetStreamingEnabled(bool value);
        TMethodDescriptor& SetPooled(bool value);
    };

    //! Per-user and per-method profiling counters.
    struct TMethodPerformanceCounters
        : public TRefCounted
    {
        explicit TMethodPerformanceCounters(const NProfiling::TProfiler& profiler);

        //! Counts the number of method calls.
        NProfiling::TCounter RequestCounter;

        //! Counts the number of canceled method calls.
        NProfiling::TCounter CanceledRequestCounter;

        //! Counts the number of failed method calls.
        NProfiling::TCounter FailedRequestCounter;

        //! Counts the number of timed out method calls.
        NProfiling::TCounter TimedOutRequestCounter;

        //! Time spent while handling the request.
        NProfiling::TEventTimer ExecutionTimeCounter;

        //! Time spent at the caller's side before the request arrived into the server queue.
        NProfiling::TEventTimer RemoteWaitTimeCounter;

        //! Time spent while the request was waiting in the server queue.
        NProfiling::TEventTimer LocalWaitTimeCounter;

        //! Time between the request arrival and the moment when it is fully processed.
        NProfiling::TEventTimer TotalTimeCounter;

        //! CPU time spent in the handler's fiber.
        NProfiling::TTimeCounter HandlerFiberTimeCounter;

        //! CPU time spent in the trace context associated with the request (locally).
        NProfiling::TTimeCounter TraceContextTimeCounter;

        //! Counts the number of bytes in requests message body.
        NProfiling::TCounter RequestMessageBodySizeCounter;

        //! Counts the number of bytes in request message attachment.
        NProfiling::TCounter RequestMessageAttachmentSizeCounter;

        //! Counts the number of bytes in response message body.
        NProfiling::TCounter ResponseMessageBodySizeCounter;

        //! Counts the number of bytes in response message attachment.
        NProfiling::TCounter ResponseMessageAttachmentSizeCounter;
    };

    using TMethodPerformanceCountersPtr = TIntrusivePtr<TMethodPerformanceCounters>;

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TRefCounted
    {
        TRuntimeMethodInfo(
            const TMethodDescriptor& descriptor,
            const NProfiling::TProfiler& profiler);

        TMethodDescriptor Descriptor;
        const NProfiling::TProfiler Registry;

        std::atomic<bool> Heavy = false;

        std::atomic<int> QueueSizeLimit = 0;
        std::atomic<int> QueueSize = 0;

        std::atomic<int> ConcurrencyLimit = 0;
        std::atomic<int> ConcurrencySemaphore = 0;
        TLockFreeQueue<TServiceContextPtr> RequestQueue;

        std::atomic<NLogging::ELogLevel> LogLevel = {};
        std::atomic<TDuration> LoggingSuppressionTimeout = {};

        NConcurrency::IReconfigurableThroughputThrottlerPtr RequestBytesThrottler;
        std::atomic<bool> RequestBytesThrottlerSpecified = false;

        NConcurrency::TSyncMap<TString, TMethodPerformanceCountersPtr> UserTagToPerformanceCounters;
        TMethodPerformanceCountersPtr RootPerformanceCounters;
        TMethodPerformanceCountersPtr GlobalPerformanceCounters;

        NConcurrency::IReconfigurableThroughputThrottlerPtr LoggingSuppressionFailedRequestThrottler;

        std::atomic<ERequestTracingMode> TracingMode = ERequestTracingMode::Enable;
    };

    using TRuntimeMethodInfoPtr = TIntrusivePtr<TRuntimeMethodInfo>;

    DECLARE_RPC_SERVICE_METHOD(NProto, Discover);

    //! Initializes the instance.
    /*!
     *  \param defaultInvoker
     *  An invoker that will be used for serving method invocations unless
     *  configured otherwise (see #RegisterMethod).
     *
     *  \param serviceName
     *  A name of the service.
     *
     *  \param logger
     *  A logger that will be used to log various debugging information
     *  regarding service activity.
     */
    TServiceBase(
        IInvokerPtr defaultInvoker,
        const TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger,
        TRealmId realmId = NullRealmId,
        IAuthenticatorPtr authenticator = nullptr);

    //! Registers a method handler.
    //! This call is must be performed prior to service registration.
    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Register a feature as being supported by server.
    //! This call is must be performed prior to service registration.
    template <class E>
    void DeclareServerFeature(E featureId);

    //! Validates the required server features against the set of supported ones.
    //! Throws on failure.
    void ValidateRequestFeatures(const IServiceContextPtr& context);

    //! Returns a reference to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfoPtr FindMethodInfo(const TString& method);

    //! Similar to #FindMethodInfo but fails if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfo(const TString& method);

    //! Similar to #FindMethodInfo but throws if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfoOrThrow(const TString& method);

    //! Returns the default invoker passed during construction.
    const IInvokerPtr& GetDefaultInvoker() const;

    //! Called right before each method handler invocation.
    virtual void BeforeInvoke(IServiceContext* context);

    //! Used by peer discovery.
    //! Returns |true| is this service instance is up, i.e. can handle requests.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsUp(const TCtxDiscoverPtr& context);

    //! Used by peer discovery.
    //! Returns addresses of neighboring peers to be suggested to the client.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual std::vector<TString> SuggestAddresses();

    //! A typed implementation of #Configure.
    void DoConfigure(
        const TServiceCommonConfigPtr& configDefaults,
        const TServiceConfigPtr& config);

protected:
    const NLogging::TLogger Logger;

private:
    const IInvokerPtr DefaultInvoker_;
    const IAuthenticatorPtr Authenticator_;
    const TServiceDescriptor ServiceDescriptor_;
    const TServiceId ServiceId_;

    const NProfiling::TProfiler ProfilingRegistry_;

    struct TPendingPayloadsEntry
    {
        std::vector<TStreamingPayload> Payloads;
        NConcurrency::TLease Lease;
    };

    std::atomic<bool> Active_ = false;

    THashMap<TString, TRuntimeMethodInfoPtr> MethodMap_;

    THashSet<int> SupportedServerFeatureIds_;

    struct TRequestBucket
    {
        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock);
        THashMap<TRequestId, TServiceContext*> RequestIdToContext;
        THashMap<TRequestId, TPendingPayloadsEntry> RequestIdToPendingPayloads;
    };

    static constexpr size_t RequestBucketCount = 64;
    std::array<TRequestBucket, RequestBucketCount> RequestBuckets_;

    struct TReplyBusBucket
    {
        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock);
        THashMap<NYT::NBus::IBusPtr, THashSet<TServiceContext*>> ReplyBusToContexts;
    };

    static constexpr size_t ReplyBusBucketCount = 64;
    std::array<TReplyBusBucket, ReplyBusBucketCount> ReplyBusBuckets_;

    static constexpr auto DefaultPendingPayloadsTimeout = TDuration::Seconds(30);
    std::atomic<TDuration> PendingPayloadsTimeout_ = DefaultPendingPayloadsTimeout;

    std::atomic<bool> Stopped_ = false;
    const TPromise<void> StopResult_ = NewPromise<void>();
    std::atomic<int> ActiveRequestCount_ = 0;

    std::atomic<int> AuthenticationQueueSize_ = 0;
    NProfiling::TEventTimer AuthenticationTimer_;

    static constexpr auto DefaultAuthenticationQueueSizeLimit = 10'000;
    std::atomic<int> AuthenticationQueueSizeLimit_ = DefaultAuthenticationQueueSizeLimit;

    std::atomic<bool> EnablePerUserProfiling_ = false;

    struct TAcceptedRequest
    {
        TRequestId RequestId;
        NYT::NBus::IBusPtr ReplyBus;
        TRuntimeMethodInfoPtr RuntimeInfo;
        NTracing::TTraceContextPtr TraceContext;
        std::unique_ptr<NRpc::NProto::TRequestHeader> Header;
        TSharedRefArray Message;
    };

    void DoDeclareServerFeature(int featureId);
    TError DoCheckRequestCompatibility(const NRpc::NProto::TRequestHeader& header);
    TError DoCheckRequestProtocol(const NRpc::NProto::TRequestHeader& header);
    TError DoCheckRequestFeatures(const NRpc::NProto::TRequestHeader& header);

    void OnRequestTimeout(TRequestId requestId, bool aborted);
    void OnReplyBusTerminated(const NYT::NBus::IBusPtr& bus, const TError& error);

    void ReplyError(
        TError error,
        const NProto::TRequestHeader& header,
        const NYT::NBus::IBusPtr& replyBus);
    void OnRequestAuthenticated(
        const NProfiling::TWallTimer& timer,
        TAcceptedRequest acceptedRequest,
        const TErrorOr<TAuthenticationResult>& authResultOrError);
    bool IsAuthenticationNeeded(const TAcceptedRequest& acceptedRequest);
    void HandleAuthenticatedRequest(TAcceptedRequest acceptedRequest);

    static bool TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ReleaseRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void RunRequest(const TServiceContextPtr& context);

    TRequestBucket* GetRequestBucket(TRequestId requestId);
    TReplyBusBucket* GetReplyBusBucket(const NYT::NBus::IBusPtr& bus);

    void RegisterRequest(TServiceContext* context);
    void UnregisterRequest(TServiceContext* context);
    TServiceContextPtr FindRequest(TRequestId requestId);
    TServiceContextPtr DoFindRequest(TRequestBucket* bucket, TRequestId requestId);

    TPendingPayloadsEntry* DoGetOrCreatePendingPayloadsEntry(TRequestBucket* bucket, TRequestId requestId);
    std::vector<TStreamingPayload> GetAndErasePendingPayloads(TRequestId requestId);
    void OnPendingPayloadsLeaseExpired(TRequestId requestId);

    TMethodPerformanceCountersPtr CreateMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const std::optional<TString>& userTag);
    TMethodPerformanceCounters* GetMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const TString& userTag);

    void SetActive();
    void ValidateInactive();
};

DEFINE_REFCOUNTED_TYPE(TServiceBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class T>
struct TPooledObjectTraits<
    class NRpc::TTypedServiceRequest<TRequestMessage>,
    T
>
    : public TPooledObjectTraitsBase<
        typename NRpc::TTypedServiceRequest<TRequestMessage>
    >
{
    static void Clean(NRpc::TTypedServiceRequest<TRequestMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

template <class TResponseMessage, class T>
struct TPooledObjectTraits<
    class NRpc::TTypedServiceResponse<TResponseMessage>,
    T
>
    : public TPooledObjectTraitsBase<
        typename NRpc::TTypedServiceRequest<TResponseMessage>
    >
{
    static void Clean(NRpc::TTypedServiceRequest<TResponseMessage>* message)
    {
        message->Clear();
        message->Attachments().clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SERVICE_DETAIL_INL_H_
#include "service_detail-inl.h"
#undef SERVICE_DETAIL_INL_H_
