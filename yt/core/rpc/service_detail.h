#pragma once

#include "public.h"

#include "client.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "service.h"
#include "config.h"

#include <yt/core/compression/codec.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/object_pool.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/ref.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/rpc/message_format.h>

#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/tracing/trace_context.h>

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
    bool Heavy = TMethodConfig::DefaultHeavy;

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
    : public IService
{
public:
    virtual void Configure(NYTree::INodePtr configNode) override;
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

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        // Defaults.
        TMethodDescriptor(
            const TString& method,
            TLiteHandler liteHandler,
            THeavyHandler heavyHandler);

        //! Invoker used for executing the handler.
        //! If |nullptr| then the default one is used.
        IInvokerPtr Invoker;

        //! Service method name.
        TString Method;

        //! A handler that will serve lite requests.
        TLiteHandler LiteHandler;

        //! A handler that will serve heavy requests.
        THeavyHandler HeavyHandler;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

        //! Maximum number of requests in queue (both waiting and executing).
        int QueueSizeLimit = TMethodConfig::DefaultQueueSizeLimit;

        //! Maximum number of requests executing concurrently.
        int ConcurrencyLimit = TMethodConfig::DefaultConcurrencyLimit;

        //! System requests are completely transparent to derived classes;
        //! in particular, |BeforeInvoke| is not called.
        bool System = false;

        //! Log level for events emitted via |Set(Request|Response)Info|-like functions.
        NLogging::ELogLevel LogLevel = TMethodConfig::DefaultLogLevel;

        //! Logging suppression timeout for this method requests.
        TDuration LoggingSuppressionTimeout = TMethodConfig::DefaultLoggingSuppressionTimeout;

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

        TMethodDescriptor& SetInvoker(IInvokerPtr value)
        {
            Invoker = value;
            return *this;
        }

        TMethodDescriptor& SetHeavy(bool value)
        {
            Options.Heavy = value;
            return *this;
        }

        TMethodDescriptor& SetResponseCodec(NCompression::ECodec value)
        {
            Options.ResponseCodec = value;
            return *this;
        }

        TMethodDescriptor& SetQueueSizeLimit(int value)
        {
            QueueSizeLimit = value;
            return *this;
        }

        TMethodDescriptor& SetConcurrencyLimit(int value)
        {
            ConcurrencyLimit = value;
            return *this;
        }

        TMethodDescriptor& SetSystem(bool value)
        {
            System = value;
            return *this;
        }

        TMethodDescriptor& SetLogLevel(NLogging::ELogLevel value)
        {
            LogLevel = value;
            return *this;
        }

        TMethodDescriptor& SetLoggingSuppressionTimeout(TDuration timeout)
        {
            LoggingSuppressionTimeout = timeout;
            return *this;
        }

        TMethodDescriptor& SetCancelable(bool value)
        {
            Cancelable = value;
            return *this;
        }

        TMethodDescriptor& SetGenerateAttachmentChecksums(bool value)
        {
            GenerateAttachmentChecksums = value;
            return *this;
        }

        TMethodDescriptor& SetStreamingEnabled(bool value)
        {
            StreamingEnabled = value;
            return *this;
        }

        TMethodDescriptor& SetPooled(bool value)
        {
            Pooled = value;
            return *this;
        }
    };

    //! Per-user and per-method profiling counters.
    struct TMethodPerformanceCounters
        : public TIntrinsicRefCounted
    {
        explicit TMethodPerformanceCounters(const NProfiling::TTagIdList& tagIds);

        //! Counts the number of method calls.
        NProfiling::TMonotonicCounter RequestCounter;

        //! Counts the number of canceled method calls.
        NProfiling::TMonotonicCounter CanceledRequestCounter;

        //! Counts the number of failed method calls.
        NProfiling::TMonotonicCounter FailedRequestCounter;

        //! Counts the number of timed out method calls.
        NProfiling::TMonotonicCounter TimedOutRequestCounter;

        //! Time spent while handling the request.
        NProfiling::TAggregateGauge ExecutionTimeCounter;

        //! Time spent at the caller's side before the request arrived into the server queue.
        NProfiling::TAggregateGauge RemoteWaitTimeCounter;

        //! Time spent while the request was waiting in the server queue.
        NProfiling::TAggregateGauge LocalWaitTimeCounter;

        //! Time between the request arrival and the moment when it is fully processed.
        NProfiling::TAggregateGauge TotalTimeCounter;

        //! CPU time spent in the handler's fiber.
        NProfiling::TMonotonicCounter HandlerFiberTimeCounter;

        //! CPU time spent in the trace context associated with the request (locally).
        NProfiling::TMonotonicCounter TraceContextTimeCounter;

        //! Counts the number of bytes in requests message body.
        NProfiling::TMonotonicCounter RequestMessageBodySizeCounter;

        //! Counts the number of bytes in request message attachment.
        NProfiling::TMonotonicCounter RequestMessageAttachmentSizeCounter;

        //! Counts the number of bytes in response message body.
        NProfiling::TMonotonicCounter ResponseMessageBodySizeCounter;

        //! Counts the number of bytes in response message attachment.
        NProfiling::TMonotonicCounter ResponseMessageAttachmentSizeCounter;
    };

    using TMethodPerformanceCountersPtr = TIntrusivePtr<TMethodPerformanceCounters>;

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TIntrinsicRefCounted
    {
        TRuntimeMethodInfo(
            const TMethodDescriptor& descriptor,
            const NProfiling::TTagIdList& tagIds);

        TMethodDescriptor Descriptor;
        const NProfiling::TTagIdList TagIds;

        std::atomic<int> QueueSize = {0};
        NProfiling::TSimpleGauge QueueSizeCounter;
        NProfiling::TSimpleGauge QueueSizeLimitCounter;
        NProfiling::TSimpleGauge ConcurrencyCounter;
        NProfiling::TSimpleGauge ConcurrencyLimitCounter;

        std::atomic<int> ConcurrencySemaphore = {0};
        TLockFreeQueue<TServiceContextPtr> RequestQueue;

        NConcurrency::TReaderWriterSpinLock PerformanceCountersLock;
        THashMap<TString, TMethodPerformanceCountersPtr> UserToPerformanceCounters;
        TMethodPerformanceCountersPtr RootPerformanceCounters;

        NConcurrency::IReconfigurableThroughputThrottlerPtr LoggingSuppressionFailedRequestThrottler;
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

    //! Registers a method.
    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Returns a reference to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfoPtr FindMethodInfo(const TString& method);

    //! Similar to #FindMethodInfo but fails if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfo(const TString& method);

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

protected:
    const NLogging::TLogger Logger;

private:
    const IInvokerPtr DefaultInvoker_;
    const IAuthenticatorPtr Authenticator_;
    const TServiceId ServiceId_;
    const TProtocolVersion ProtocolVersion_;

    const NProfiling::TTagId ServiceTagId_;

    const NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    struct TPendingPayloadsEntry
    {
        std::vector<TStreamingPayload> Payloads;
        NConcurrency::TLease Lease;
    };

    NConcurrency::TReaderWriterSpinLock MethodMapLock_;
    THashMap<TString, TRuntimeMethodInfoPtr> MethodMap_;

    TSpinLock RequestMapLock_;
    THashMap<TRequestId, TServiceContext*> RequestIdToContext_;
    THashMap<NYT::NBus::IBusPtr, THashSet<TServiceContext*>> ReplyBusToContexts_;
    THashMap<TRequestId, TPendingPayloadsEntry> RequestIdToPendingPayloads_;
    TDuration PendingPayloadsTimeout_ = TServiceConfig::DefaultPendingPayloadsTimeout;

    std::atomic<bool> Stopped_ = {false};
    TPromise<void> StopResult_ = NewPromise<void>();
    std::atomic<int> ActiveRequestCount_ = {0};

    std::atomic<int> AuthenticationQueueSize_ = {0};
    NProfiling::TSimpleGauge AuthenticationQueueSizeCounter_;
    NProfiling::TAggregateGauge AuthenticationTimeCounter_;
    int AuthenticationQueueSizeLimit_ = TServiceConfig::DefaultAuthenticationQueueSizeLimit;

private:
    struct TAcceptedRequest
    {
        TRequestId RequestId;
        NYT::NBus::IBusPtr ReplyBus;
        TRuntimeMethodInfoPtr RuntimeInfo;
        NTracing::TTraceContextPtr TraceContext;
        std::unique_ptr<NRpc::NProto::TRequestHeader> Header;
        TSharedRefArray Message;
    };

    void OnRequestTimeout(TRequestId requestId, bool aborted);
    void OnReplyBusTerminated(NYT::NBus::IBusPtr bus, const TError& error);

    void ReplyError(
        TError error,
        const NProto::TRequestHeader& header,
        const NYT::NBus::IBusPtr& replyBus);
    void OnRequestAuthenticated(
        const NProfiling::TWallTimer& timer,
        TAcceptedRequest acceptedRequest,
        const TErrorOr<TAuthenticationResult>& authResultOrError);
    void HandleAuthenticatedRequest(TAcceptedRequest acceptedRequest);

    static bool TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ReleaseRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void RunRequest(const TServiceContextPtr& context);

    void RegisterRequest(TServiceContext* context);
    void UnregisterRequest(TServiceContext* context);
    TServiceContextPtr FindRequest(TRequestId requestId);
    TServiceContextPtr DoFindRequest(TRequestId requestId);

    TPendingPayloadsEntry* DoGetOrCreatePendingPayloadsEntry(TRequestId requestId);
    std::vector<TStreamingPayload> GetAndErasePendingPayloads(TRequestId requestId);
    void OnPendingPayloadsLeaseExpired(TRequestId requestId);

    TMethodPerformanceCountersPtr CreateMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const TString& user);
    TMethodPerformanceCounters* GetMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const TString& user);

    void OnProfiling();
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
