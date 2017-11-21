#pragma once

#include "public.h"
#include "client.h"
#include "dispatcher.h"
#include "server_detail.h"
#include "service.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/object_pool.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/ref.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/rpc/rpc.pb.h>

#include <yt/core/tracing/trace_context.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT {
namespace NRpc {

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

    std::vector<TSharedRef>& Attachments()
    {
        return Context_->RequestAttachments();
    }

private:
    template <class TRequestMessage_, class TResponseMessage_>
    friend class TTypedServiceContext;

    IServiceContext* Context_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    using TMessage = TResponseMessage;

    std::vector<TSharedRef>& Attachments()
    {
        return Context_->ResponseAttachments();
    }

private:
    template <class TRequestMessage_, class TResponseMessage_>
    friend class TTypedServiceContext;

    IServiceContext* Context_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

//! Describes request handling options.
struct THandlerInvocationOptions
{
    //! Should we be deserializing the request and serializing the request
    //! in a separate thread?
    bool Heavy = false;

    //! The codec to compress response body.
    NCompression::ECodec ResponseCodec = NCompression::ECodec::None;
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
        Response_ = ObjectPool<TTypedResponse>().Allocate();
        Response_->Context_ = this->UnderlyingContext_.Get();
    }

    bool DeserializeRequest()
    {
        Request_ = ObjectPool<TTypedRequest>().Allocate();
        Request_->Context_ = UnderlyingContext_.Get();

        if (!TryDeserializeFromProtoWithEnvelope(Request_.get(), UnderlyingContext_->GetRequestBody())) {
            UnderlyingContext_->Reply(TError(
                NRpc::EErrorCode::ProtocolError,
                "Error deserializing request body"));
            return false;
        }

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
    THandlerInvocationOptions Options_;
    typename TObjectPool<TTypedRequest>::TObjectPtr Request_;
    typename TObjectPool<TTypedResponse>::TObjectPtr Response_;

    void DoReply(const TError& error)
    {
        if (error.IsOK()) {
            auto data = SerializeToProtoWithEnvelope(*Response_, this->Options_.ResponseCodec, false);
            this->UnderlyingContext_->SetResponseBody(std::move(data));
        }
        this->UnderlyingContext_->Reply(error);
        this->Request_.reset();
        this->Response_.reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    using TCtx##method = ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method>; \
    using TCtx##method##Ptr = ::NYT::TIntrusivePtr<TCtx##method>; \
    using TReq##method = TCtx##method::TTypedRequest; \
    using TRsp##method = TCtx##method::TTypedResponse ; \
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
        NBus::IBusPtr replyBus) override;

    virtual void HandleRequestCancelation(const TRequestId& requestId) override;

protected:
    using TLiteHandler = TCallback<void(const IServiceContextPtr&, const THandlerInvocationOptions&)>;
    using THeavyHandler = TCallback<TLiteHandler(const IServiceContextPtr&, const THandlerInvocationOptions&)>;

    class TServiceContext;
    using TServiceContextPtr = TIntrusivePtr<TServiceContext>;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
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

        //! Is the method one-way?
        bool OneWay = false;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

        //! Maximum number of requests in queue (both waiting and executing).
        int MaxQueueSize = 10000;

        //! Maximum number of requests executing concurrently.
        int MaxConcurrency = 1000;

        //! System requests are completely transparent to derived classes;
        //! in particular, |BeforeInvoke| is not called.
        bool System = false;

        //! Log level for events emitted via |Set(Request|Response)Info|-like functions.
        NLogging::ELogLevel LogLevel = NLogging::ELogLevel::Debug;

        //! Cancelable requests can be canceled by clients.
        //! This, however, requires additional book-keeping at server-side so one is advised
        //! to only mark cancelable those methods taking a considerable time to complete.
        bool Cancelable = false;

        //! If |true| then Bus is expected to be gerating checksums for the whole response content,
        //! including attachments (unless the connection is local or the checksums are explicitly disabled).
        //! If |false| then Bus will only be generating such checksums for RPC header and response body
        //! but not attachements.
        bool GenerateAttachmentChecksums = true;


        TMethodDescriptor& SetInvoker(IInvokerPtr value)
        {
            Invoker = value;
            return *this;
        }

        TMethodDescriptor& SetOneWay(bool value)
        {
            OneWay = value;
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

        TMethodDescriptor& SetMaxQueueSize(int value)
        {
            MaxQueueSize = value;
            return *this;
        }

        TMethodDescriptor& SetMaxConcurrency(int value)
        {
            MaxConcurrency = value;
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
    };

    //! Per-user and per-method profiling counters.
    struct TMethodPerformanceCounters
        : public TIntrinsicRefCounted
    {
        explicit TMethodPerformanceCounters(const NProfiling::TTagIdList& tagIds);

        //! Counts the number of method calls.
        NProfiling::TSimpleCounter RequestCounter;

        //! Counts the number of canceled method calls.
        NProfiling::TSimpleCounter CanceledRequestCounter;

        //! Counts the number of timed out method calls.
        NProfiling::TSimpleCounter TimedOutRequestCounter;

        //! Time spent while handling the request.
        NProfiling::TAggregateCounter ExecutionTimeCounter;

        //! Time spent at the caller's side before the request arrived into the server queue.
        NProfiling::TAggregateCounter RemoteWaitTimeCounter;

        //! Time spent while the request was waiting in the server queue.
        NProfiling::TAggregateCounter LocalWaitTimeCounter;

        //! Time between the request arrival and the moment when it is fully processed.
        NProfiling::TAggregateCounter TotalTimeCounter;
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

        //! The number of currently queued requests.
        NProfiling::TAggregateCounter QueueSizeCounter;

        std::atomic<int> ConcurrencySemaphore = {0};
        TLockFreeQueue<TServiceContextPtr> RequestQueue;

        NConcurrency::TReaderWriterSpinLock PerformanceCountersLock;
        THashMap<TString, TMethodPerformanceCountersPtr> UserToPerformanceCounters;
        TMethodPerformanceCountersPtr RootPerformanceCounters;
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
        const TRealmId& realmId = NullRealmId);

    //! Registers a method.
    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Returns a reference to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfoPtr FindMethodInfo(const TString& method);

    //! Similar to #FindMethodInfo but fails if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfo(const TString& method);

    //! Returns the default invoker passed during construction.
    IInvokerPtr GetDefaultInvoker();

    //! Called right before each method handler invocation.
    virtual void BeforeInvoke();

    //! Used by peer discovery.
    //! Returns |true| is this service instance is up, i.e. can handle requests.
    /*!
     *  \note
     *  Thread affinity: any
     */
    virtual bool IsUp(TCtxDiscoverPtr context);

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
    const TServiceId ServiceId_;
    const int ProtocolVersion_;

    NProfiling::TTagId ServiceTagId_;

    NConcurrency::TReaderWriterSpinLock MethodMapLock_;
    THashMap<TString, TRuntimeMethodInfoPtr> MethodMap_;

    TSpinLock CancelableRequestLock_;
    THashMap<TRequestId, TServiceContext*> IdToContext_;
    THashMap<NBus::IBusPtr, THashSet<TServiceContext*>> ReplyBusToContexts_;

    std::atomic<bool> Stopped_ = {false};
    TPromise<void> StopResult_ = NewPromise<void>();
    std::atomic<int> ActiveRequestCount_ = {0};


    void OnRequestTimeout(const TRequestId& requestId, bool aborted);
    void OnReplyBusTerminated(NBus::IBusPtr bus, const TError& error);

    static bool TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ReleaseRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void RunRequest(const TServiceContextPtr& context);

    void RegisterCancelableRequest(TServiceContext* context);
    void UnregisterCancelableRequest(TServiceContext* context);
    TServiceContextPtr FindCancelableRequest(const TRequestId& requestId);

    TMethodPerformanceCountersPtr CreateMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const TString& user);

    TMethodPerformanceCounters* LookupMethodPerformanceCounters(
        const TRuntimeMethodInfoPtr& runtimeInfo,
        const TString& user);

};

DEFINE_REFCOUNTED_TYPE(TServiceBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
