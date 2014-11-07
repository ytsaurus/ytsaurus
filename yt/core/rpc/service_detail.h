#pragma once

#include "public.h"
#include "service.h"
#include "server_detail.h"
#include "dispatcher.h"

#include <core/misc/ref.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/action_queue.h>

#include <core/misc/object_pool.h>

#include <core/compression/codec.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

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

    TTypedServiceRequest()
        : Context(nullptr)
    { }

    std::vector<TSharedRef>& Attachments()
    {
        return Context->RequestAttachments();
    }

    NYTree::IAttributeDictionary& Attributes() const
    {
        return Context->RequestAttributes();
    }

private:
    template <class TRequestMessage_>
    friend class TTypedServiceContextBase;

    IServiceContext* Context;

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    typedef TResponseMessage TMessage;

    TTypedServiceResponse()
        : Context(nullptr)
    { }

    std::vector<TSharedRef>& Attachments()
    {
        return Context->ResponseAttachments();
    }

    NYTree::IAttributeDictionary& Attributes()
    {
        return Context->ResponseAttributes();
    }

private:
    template <class TRequestMessage_, class TResponseMessage_>
    friend class TTypedServiceContext;

    IServiceContext* Context;

};

////////////////////////////////////////////////////////////////////////////////

//! Describes request handling options.
struct THandlerInvocationOptions
{
    THandlerInvocationOptions()
        : HeavyRequest(false)
        , HeavyResponse(false)
        , ResponseCodec(NCompression::ECodec::None)
    { }

    //! Should we be deserializing the request in a separate thread?
    bool HeavyRequest;

    //! Should we be serializing the response in a separate thread?
    bool HeavyResponse;

    //! The codec to compress response body.
    NCompression::ECodec ResponseCodec;

};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern NLog::TLogger RpcServerLogger;

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
        , Options(options)
    { }

    bool DeserializeRequest()
    {
        Request_ = ObjectPool<TTypedRequest>().Allocate();
        Request_->Context = UnderlyingContext.Get();

        if (!DeserializeFromProtoWithEnvelope(Request_.get(), UnderlyingContext->GetRequestBody())) {
            UnderlyingContext->Reply(TError(
                EErrorCode::ProtocolError,
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
    NLog::TLogger& Logger;
    THandlerInvocationOptions Options;

    typename TObjectPool<TTypedRequest>::TValuePtr Request_;

};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern NLog::TLogger RpcServerLogger;

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
        Response_->Context = this->UnderlyingContext.Get();
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
            this->UnderlyingContext->Reply(error);
            return;
        }

        if (this->Options.HeavyResponse) {
            TDispatcher::Get()->GetPoolInvoker()->Invoke(BIND(
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
        YCHECK(SerializeToProtoWithEnvelope(*Response_, &data, this->Options.ResponseCodec));
        this->UnderlyingContext->SetResponseBody(std::move(data));
        this->UnderlyingContext->Reply(TError());
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

//! Provides a base for implementing IService.
class TServiceBase
    : public IService
{
protected:
    //! Describes a handler for a service method.
    typedef TCallback<TClosure(IServiceContextPtr, const THandlerInvocationOptions&)> THandler;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        TMethodDescriptor(const Stroka& verb, THandler handler);

        //! Invoker used to executing the handler.
        //! If |nullptr| then the default one is used.
        IPrioritizedInvokerPtr Invoker;

        //! Service method name.
        Stroka Verb;

        //! A handler that will serve the requests.
        THandler Handler;

        //! Is the method one-way?
        bool OneWay;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

        //! Maximum number of concurrent requests waiting in queue.
        int MaxQueueSize;

        //! Should requests be reordered based on start time?
        bool EnableReorder;

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

        TMethodDescriptor& SetEnableReorder(bool value)
        {
            EnableReorder = value;
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

    };

    typedef TIntrusivePtr<TRuntimeMethodInfo> TRuntimeMethodInfoPtr;

    //! A request that is currently being served.
    struct TActiveRequest
        : public TIntrinsicRefCounted
    {
        TActiveRequest(
            const TRequestId& id,
            NBus::IBusPtr replyBus,
            TRuntimeMethodInfoPtr runtimeInfo);

        //! Request id.
        TRequestId Id;

        //! Bus for replying back to the client.
        NBus::IBusPtr ReplyBus;

        //! Method that is being served.
        TRuntimeMethodInfoPtr RuntimeInfo;

        //! Guards the rest.
        TSpinLock SpinLock;

        //! True if the service method is currently running synchronously.
        bool RunningSync;

        //! True if #OnEndRequest is already called.
        bool Completed;

        //! The moment when the request arrived to the server.
        NProfiling::TCpuInstant ArrivalTime;

        //! The moment when the request was dequeued and its synchronous
        //! execution started.
        NProfiling::TCpuInstant SyncStartTime;

        //! The moment when the synchronous part of the execution finished.
        NProfiling::TCpuInstant SyncStopTime;

    };

    typedef TIntrusivePtr<TActiveRequest> TActiveRequestPtr;

    //! Initializes the instance.
    /*!
     *  \param defaultInvoker
     *  An invoker that will be used for serving method invocations unless
     *  configured otherwise (see #RegisterMethod).
     *
     *  \param serviceName
     *  A name of the service.
     *
     *  \param loggingCategory
     *  A category that will be used to log various debugging information
     *  regarding service activity.
     */
    TServiceBase(
        IPrioritizedInvokerPtr defaultInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    TServiceBase(
        IInvokerPtr defaultInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    //! Registers a method.
    TRuntimeMethodInfoPtr RegisterMethod(const TMethodDescriptor& descriptor);

    //! Configures the service.
    virtual void Configure(NYTree::INodePtr configNode) override;

    //! Prepares the handler to invocation.
    virtual TClosure PrepareHandler(TClosure handler);

    //! Replies #error to every request in #ActiveRequests, clears the latter one.
    void CancelActiveRequests(const TError& error);

    //! Returns a reference to TRuntimeMethodInfo for a given method's name
    //! or |nullptr| if no such method is registered.
    TRuntimeMethodInfoPtr FindMethodInfo(const Stroka& method);

    //! Similar to #FindMethodInfo but fails if no method is found.
    TRuntimeMethodInfoPtr GetMethodInfo(const Stroka& method);

    //! Returns the default invoker passed during construction.
    IPrioritizedInvokerPtr GetDefaultInvoker();


private:
    class TServiceContext;

    IPrioritizedInvokerPtr DefaultInvoker;
    Stroka ServiceName;
    Stroka LoggingCategory;

    NProfiling::TTagId ServiceTagId;
    NProfiling::TRateCounter RequestCounter;

    //! Protects the following data members.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfoPtr> RuntimeMethodInfos;
    yhash_set<TActiveRequestPtr> ActiveRequests;

    void Init(
        IPrioritizedInvokerPtr defaultInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    virtual Stroka GetServiceName() const override;

    virtual void OnRequest(
        const NProto::TRequestHeader& header,
        TSharedRefArray message,
        NBus::IBusPtr replyBus) override;

    void OnInvocationPrepared(
        TActiveRequestPtr activeRequest,
        IServiceContextPtr context,
        TClosure handler);

    void OnResponse(TActiveRequestPtr activeRequest, TSharedRefArray message);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    TClosure method##Thunk( \
        ::NYT::NRpc::IServiceContextPtr context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(std::move(context), options); \
        if (!typedContext->DeserializeRequest()) { \
            return TClosure(); \
        } \
        return BIND([=] () { \
            this->method( \
                &typedContext->Request(), \
                &typedContext->Response(), \
                typedContext); \
        }); \
    } \
    \
    void method( \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method##Ptr context)

#define DEFINE_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method##Ptr context)

#define RPC_SERVICE_METHOD_DESC(method) \
    ::NYT::NRpc::TServiceBase::TMethodDescriptor( \
        #method, \
        BIND(&TThis::method##Thunk, ::NYT::Unretained(this)))

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_ONE_WAY_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayTypedServiceContext<ns::TReq##method> TCtx##method; \
    typedef ::NYT::TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    \
    TClosure method##Thunk( \
        ::NYT::NRpc::IServiceContextPtr context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = ::NYT::New<TCtx##method>(std::move(context), options); \
        if (!typedContext->DeserializeRequest()) { \
            return TClosure(); \
        } \
        return BIND([=] () { \
            this->method( \
                &typedContext->Request(), \
                typedContext); \
        }); \
    } \
    \
    void method( \
        TReq##method* request, \
        TCtx##method##Ptr context)

#define DEFINE_ONE_WAY_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TCtx##method##Ptr context)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
