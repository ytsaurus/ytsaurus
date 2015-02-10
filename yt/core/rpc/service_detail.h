#pragma once

#include "public.h"
#include "service.h"
#include "server_detail.h"
#include "dispatcher.h"
#include "client.h"

#include <core/misc/ref.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/object_pool.h>

#include <core/concurrency/rw_spinlock.h>
#include <core/concurrency/action_queue.h>

#include <core/compression/codec.h>

#include <core/rpc/rpc.pb.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <core/tracing/trace_context.h>

#include <atomic>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage>
class TTypedServiceContextBase;

template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext;

template <class TRequestMessage>
class TOneWayTypedServiceContext;

template <class TRequestMessage>
class TTypedServiceRequest
    : public TRequestMessage
{
public:
    typedef TRequestMessage TMessage;

    std::vector<TSharedRef>& Attachments()
    {
        return Context_->RequestAttachments();
    }

private:
    template <class TRequestMessage_>
    friend class TTypedServiceContextBase;

    IServiceContext* Context_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    typedef TResponseMessage TMessage;

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
    //! Should we be deserializing the request in a separate thread?
    bool HeavyRequest = false;

    //! Should we be serializing the response in a separate thread?
    bool HeavyResponse = false;

    //! The codec to compress response body.
    NCompression::ECodec ResponseCodec = NCompression::ECodec::None;
};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern const NLog::TLogger RpcServerLogger;

//! Provides a common base for both one-way and two-way contexts.
template <class TRequestMessage>
class TTypedServiceContextBase
    : public TServiceContextWrapper
{
public:
    typedef TTypedServiceRequest<TRequestMessage> TTypedRequest;

    explicit TTypedServiceContextBase(
        IServiceContextPtr context,
        const THandlerInvocationOptions& options)
        : TServiceContextWrapper(std::move(context))
        , Logger(RpcServerLogger)
        , Options_(options)
    { }

    bool DeserializeRequest()
    {
        Request_ = ObjectPool<TTypedRequest>().Allocate();
        Request_->Context_ = UnderlyingContext_.Get();

        if (!DeserializeFromProtoWithEnvelope(Request_.get(), UnderlyingContext_->GetRequestBody())) {
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

protected:
    NLog::TLogger Logger;
    THandlerInvocationOptions Options_;

    typename TObjectPool<TTypedRequest>::TValuePtr Request_;

};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern const NLog::TLogger RpcServerLogger;

//! Describes a two-way context.
template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext
    : public TTypedServiceContextBase<TRequestMessage>
{
public:
    typedef TTypedServiceContext<TRequestMessage, TResponseMessage> TThis;
    typedef TTypedServiceContextBase<TRequestMessage> TBase;
    typedef TTypedServiceResponse<TResponseMessage> TTypedResponse;

    explicit TTypedServiceContext(
        IServiceContextPtr context,
        const THandlerInvocationOptions& options)
        : TBase(std::move(context), options)
    {
        Response_ = ObjectPool<TTypedResponse>().Allocate();
        Response_->Context_ = this->UnderlyingContext_.Get();
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
        if (!error.IsOK()) {
            this->UnderlyingContext_->Reply(error);
            return;
        }

        if (this->Options_.HeavyResponse) {
            TDispatcher::Get()->GetInvoker()->Invoke(BIND(
                &TThis::SerializeResponseAndReply,
                MakeStrong(this)));
        } else {
            this->SerializeResponseAndReply();
        }
    }

private:
    void SerializeResponseAndReply()
    {
        TSharedRef data;
        YCHECK(SerializeToProtoWithEnvelope(*Response_, &data, this->Options_.ResponseCodec));
        this->UnderlyingContext_->SetResponseBody(std::move(data));
        this->UnderlyingContext_->Reply(TError());
    }

    typename TObjectPool<TTypedResponse>::TValuePtr Response_;

};

////////////////////////////////////////////////////////////////////////////////

//! Describes a one-way context.
template <class TRequestMessage>
class TOneWayTypedServiceContext
    : public TTypedServiceContextBase<TRequestMessage>
{
public:
    typedef TOneWayTypedServiceContext<TRequestMessage> TThis;
    typedef TTypedServiceContextBase<TRequestMessage> TBase;

    explicit TOneWayTypedServiceContext(
        IServiceContextPtr context,
        const THandlerInvocationOptions& options)
        : TBase(std::move(context), options)
    { }

};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_RPC_SERVICE_METHOD_THUNK(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    void method##LiteThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(context, options); \
        if (!typedContext->DeserializeRequest()) \
            return; \
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

#define DECLARE_ONE_WAY_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayTypedServiceContext<ns::TReq##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest TReq##method; \
    \
    void method##LiteThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(std::move(context), options); \
        if (!typedContext->DeserializeRequest()) \
            return; \
        auto* request = &typedContext->Request(); \
        this->method(request, typedContext); \
    } \
    \
    ::NYT::NRpc::TServiceBase::TLiteHandler method##HeavyThunk( \
        const ::NYT::NRpc::IServiceContextPtr& context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(std::move(context), options); \
        if (!typedContext->DeserializeRequest()) { \
            return ::NYT::NRpc::TServiceBase::TLiteHandler(); \
        } \
        return \
            BIND([=] ( \
                const ::NYT::NRpc::IServiceContextPtr&, \
                const ::NYT::NRpc::THandlerInvocationOptions&) \
            { \
                auto* request = &typedContext->Request(); \
                this->method(request, typedContext); \
            }); \
    } \
    \
    void method( \
        TReq##method* request, \
        const TCtx##method##Ptr& context)

#define DEFINE_ONE_WAY_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        const TCtx##method##Ptr& context)

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public IService
{
protected:
    typedef TCallback<void(const IServiceContextPtr&, const THandlerInvocationOptions&)> TLiteHandler;
    typedef TCallback<TLiteHandler(const IServiceContextPtr&, const THandlerInvocationOptions&)> THeavyHandler;

    class TServiceContext;
    typedef TIntrusivePtr<TServiceContext> TServiceContextPtr;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        TMethodDescriptor(
            const Stroka& method,
            TLiteHandler liteHandler,
            THeavyHandler heavyHandler);

        //! Invoker used for executing the handler.
        //! If |nullptr| then the default one is used.
        IPrioritizedInvokerPtr Invoker;

        //! Service method name.
        Stroka Method;

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

        //! Should requests be reordered based on start time?
        bool EnableReorder = false;

        //! System requests are completely transparent to derived classes;
        //! in particular, |BeforeInvoke| is not called.
        bool System = false;

        //! Log level for events emitted via |Set(Request|Response)Info|-like functions.
        NLog::ELogLevel LogLevel = NLog::ELogLevel::Debug;

        //! Cancelable requests can be canceled by clients.
        //! This, however, requires additional book-keeping at server-side so one is advised
        //! to only mark cancelable those methods taking a considerable time to complete.
        bool Cancelable = false;


        TMethodDescriptor& SetInvoker(IPrioritizedInvokerPtr value)
        {
            Invoker = value;
            return *this;
        }

        TMethodDescriptor& SetInvoker(IInvokerPtr value)
        {
            Invoker = NConcurrency::CreateFakePrioritizedInvoker(value);
            return *this;
        }

        TMethodDescriptor& SetOneWay(bool value)
        {
            OneWay = value;
            return *this;
        }

        TMethodDescriptor& SetRequestHeavy(bool value)
        {
            Options.HeavyRequest = value;
            return *this;
        }

        TMethodDescriptor& SetResponseHeavy(bool value)
        {
            Options.HeavyResponse = value;
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

        TMethodDescriptor& SetEnableReorder(bool value)
        {
            EnableReorder = value;
            return *this;
        }

        TMethodDescriptor& SetSystem(bool value)
        {
            System = value;
            return *this;
        }

        TMethodDescriptor& SetLogLevel(NLog::ELogLevel value)
        {
            LogLevel = value;
            return *this;
        }

        TMethodDescriptor& SetCancelable(bool value)
        {
            Cancelable = value;
            return *this;
        }
    };

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TIntrinsicRefCounted
    {
        explicit TRuntimeMethodInfo(
            const TMethodDescriptor& descriptor,
            const NProfiling::TTagIdList& tagIds);

        TMethodDescriptor Descriptor;

        //! Counts the number of method calls.
        NProfiling::TAggregateCounter RequestCounter;

        //! The number of currently queued requests.
        NProfiling::TAggregateCounter QueueSizeCounter;

        //! Time spent while handling the request synchronously.
        NProfiling::TAggregateCounter SyncTimeCounter;

        //! Time spent while handling the request asynchronously.
        NProfiling::TAggregateCounter AsyncTimeCounter;

        //! Time spent at the caller's side before the request arrived into the server queue.
        NProfiling::TAggregateCounter RemoteWaitTimeCounter;

        //! Time spent while the request was waiting in the server queue.
        NProfiling::TAggregateCounter LocalWaitTimeCounter;

        //! Time between the request arrival and the moment when it is fully processed.
        NProfiling::TAggregateCounter TotalTimeCounter;

        std::atomic<int> RunningRequestSemaphore = {0};
        TLockFreeQueue<TServiceContextPtr> RequestQueue;
    };

    typedef TIntrusivePtr<TRuntimeMethodInfo> TRuntimeMethodInfoPtr;

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
        IPrioritizedInvokerPtr defaultInvoker,
        const TServiceId& serviceId,
        const NLog::TLogger& logger,
        int protocolVersion = TProxyBase::DefaultProtocolVersion,
        IResponseKeeperPtr responseKeeper = nullptr);

    TServiceBase(
        IInvokerPtr defaultInvoker,
        const TServiceId& serviceId,
        const NLog::TLogger& logger,
        int protocolVersion = TProxyBase::DefaultProtocolVersion,
        IResponseKeeperPtr responseKeeper = nullptr);

    //! Registers a method.
    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Configures the service.
    virtual void Configure(NYTree::INodePtr configNode) override;

    //! Returns a reference to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfoPtr FindMethodInfo(const Stroka& method);

    //! Similar to #FindMethodInfo but fails if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfo(const Stroka& method);

    //! Returns the default invoker passed during construction.
    IPrioritizedInvokerPtr GetDefaultInvoker();

    //! Called right before each method handler invocation.
    virtual void BeforeInvoke();

    //! Used by peer discovery.
    //! Returns |true| is this service instance is up, i.e. can handle requests.
    /*!
     *  \note
     *  Thread affinity: any
     */ 
    virtual bool IsUp(TCtxDiscoverPtr context) const;

    //! Used by peer discovery.
    //! Returns addresses of neighboring peers to be suggested to the client.
    /*!
     *  \note
     *  Thread affinity: any
     */ 
    virtual std::vector<Stroka> SuggestAddresses() const;

protected:
    NLog::TLogger Logger;

private:
    IPrioritizedInvokerPtr DefaultInvoker_;
    TServiceId ServiceId_;
    int ProtocolVersion_;
    IResponseKeeperPtr ResponseKeeper_;

    NProfiling::TTagId ServiceTagId_;
    NProfiling::TRateCounter RequestCounter_;

    NConcurrency::TReaderWriterSpinLock MethodMapLock_;
    yhash_map<Stroka, TRuntimeMethodInfoPtr> MethodMap_;

    TSpinLock CancelableRequestLock_;
    yhash_map<TRequestId, TServiceContext*> IdToContext_;
    yhash_map<NBus::IBusPtr, yhash_set<TServiceContext*>> ReplyBusToContexts_;

    void Init(
        IPrioritizedInvokerPtr defaultInvoker,
        const TServiceId& serviceId,
        const NLog::TLogger& logger,
        int protocolVersion,
        IResponseKeeperPtr responseKeeper);

    virtual TServiceId GetServiceId() const override;

    virtual void HandleRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        NBus::IBusPtr replyBus) override;

    virtual void HandleRequestCancelation(const TRequestId& requestId) override;

    void OnRequestTimeout(const TRequestId& requestId);
    void OnReplyBusTerminated(NBus::IBusPtr bus, const TError& error);

    static bool TryAcquireRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ReleaseRequestSemaphore(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void ScheduleRequests(const TRuntimeMethodInfoPtr& runtimeInfo);
    static void RunRequest(const TServiceContextPtr& context);

    void RegisterCancelableRequest(TServiceContext* context);
    void UnregisterCancelableRequest(TServiceContext* context);
    TServiceContextPtr FindCancelableRequest(const TRequestId& requestId);

};

DEFINE_REFCOUNTED_TYPE(TServiceBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
