#pragma once

#include "public.h"
#include "client.h"
#include "dispatcher.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/hash.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/error.h>
#include <ytlib/misc/object_pool.h>

#include <ytlib/codecs/codec.h>

#include <ytlib/rpc/rpc.pb.h>

#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Represents an RPC request at server-side.
struct IServiceContext
    : public virtual TRefCounted
{
    //! Returns the message that contains the request being handled.
    virtual NBus::IMessagePtr GetRequestMessage() const = 0;

    //! Returns the id of the request.
    /*!
     *  These ids are assigned by the client to distinguish between responses.
     *  The server should not rely on their uniqueness.
     *  #NullRequestId is a possible value.
     */
    virtual const TRequestId& GetRequestId() const = 0;

    //! Returns the requested path.
    virtual const Stroka& GetPath() const = 0;

    //! Returns the requested verb.
    virtual const Stroka& GetVerb() const = 0;

    //! Returns True if the request if one-way, i.e. replying to it is not possible.
    virtual bool IsOneWay() const = 0;

    //! Returns True if the request was already replied.
    virtual bool IsReplied() const = 0;

    //! Signals that the request processing is complete and sends reply to the client.
    virtual void Reply(const TError& error) = 0;

    //! Parses the message and forwards to the client.
    virtual void Reply(NBus::IMessagePtr message) = 0;

    //! Returns the error that was previously set by #Reply.
    /*!
     *  Calling #GetError before #Reply is forbidden.
     */
    virtual const TError& GetError() const = 0;

    //! Returns the request body.
    virtual TSharedRef GetRequestBody() const = 0;

    //! Returns the response body.
    virtual TSharedRef GetResponseBody() = 0;

    //! Sets the response body.
    virtual void SetResponseBody(const TSharedRef& responseBody) = 0;

    //! Returns a vector of request attachments.
    virtual std::vector<TSharedRef>& RequestAttachments() = 0;

    //! Returns request attributes.
    virtual NYTree::IAttributeDictionary& RequestAttributes() = 0;

    //! Returns a vector of response attachments.
    virtual std::vector<TSharedRef>& ResponseAttachments() = 0;

    //! Returns response attributes.
    virtual NYTree::IAttributeDictionary& ResponseAttributes() = 0;

    //! Sets and immediately logs the request logging info.
    virtual void SetRequestInfo(const Stroka& info) = 0;

    //! Returns the previously set request logging info.
    virtual Stroka GetRequestInfo() const = 0;

    //! Sets the response logging info. This info will be logged when the context is replied.
    virtual void SetResponseInfo(const Stroka& info) = 0;

    //! Returns the currently set response logging info.
    virtual Stroka GetResponseInfo() = 0;

    //! Wraps the given action into an exception guard that logs the exception and replies.
    virtual TClosure Wrap(const TClosure& action) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : public virtual TRefCounted
{
    virtual Stroka GetServiceName() const = 0;
    
    virtual void OnRequest(
        const NProto::TRequestHeader& header,
        NBus::IMessagePtr message,
        NBus::IBusPtr replyBus) = 0;
};

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
    TTypedServiceRequest()
        : Context(NULL)
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
    TTypedServiceResponse()
        : Context(NULL)
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
        , ResponseCodec(ECodecId::None)
    { }

    //! Should we be deserializing the request in a separate thread?
    bool HeavyRequest;

    //! Should we be serializing the response in a separate thread?
    bool HeavyResponse;

    //! The codec to compress response body.
    ECodecId ResponseCodec;

};

////////////////////////////////////////////////////////////////////////////////

// We need this logger here but including the whole private.h looks weird.
extern NLog::TLogger RpcServerLogger;

//! Provides a common base for both one-way and two-way contexts.
template <class TRequestMessage>
class TTypedServiceContextBase
    : public TRefCounted
{
public:
    typedef TTypedServiceRequest<TRequestMessage> TTypedRequest;

    explicit TTypedServiceContextBase(
        IServiceContextPtr context,
        const THandlerInvocationOptions& options)
        : Logger(RpcServerLogger)
        , Context(MoveRV(context))
        , Options(options)
    {
        YCHECK(Context);
    }

    void DeserializeRequest()
    {
        Request_ = ObjectPool<TTypedRequest>().Allocate();
        Request_->Context = Context.Get();

        if (!DeserializeFromProtoWithEnvelope(Request_.Get(), Context->GetRequestBody())) {
            THROW_ERROR_EXCEPTION(
                EErrorCode::ProtocolError,
                "Error deserializing request body");
        }
    }

    const TTypedRequest& Request() const
    {
        return *Request_;
    }

    TTypedRequest& Request()
    {
        return *Request_;
    }

    const Stroka& GetPath() const
    {
        return Context->GetPath();
    }

    const Stroka& GetVerb() const
    {
        return Context->GetVerb();
    }

    void SetRequestInfo(const Stroka& info)
    {
        Context->SetRequestInfo(info);
    }

    void SetRequestInfo(const char* format, ...)
    {
        Stroka info;
        va_list params;
        va_start(params, format);
        vsprintf(info, format, params);
        va_end(params);
        Context->SetRequestInfo(info);
    }

    Stroka GetRequestInfo() const
    {
        return Context->GetRequestInfo();
    }

    IServiceContextPtr GetUntypedContext() const
    {
        return Context;
    }

    TClosure Wrap(const TClosure& action)
    {
        YASSERT(!action.IsNull());
        return Context->Wrap(action);
    }

protected:
    NLog::TLogger& Logger;
    IServiceContextPtr Context;
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
        : TBase(MoveRV(context), options)
    {
        Response_ = ObjectPool<TTypedResponse>().Allocate();
        Response_->Context = this->Context.Get();
    }

    const TTypedResponse& Response() const
    {
        return *Response_;
    }

    TTypedResponse& Response()
    {
        return *Response_;
    }

    void Reply()
    {
        Reply(TError());
    }

    void Reply(const TError& error)
    {
        if (!error.IsOK()) {
            this->Context->Reply(error);
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

    bool IsReplied() const
    {
        return this->Context->IsReplied();
    }

    void SetResponseInfo(const Stroka& info)
    {
        this->Context->SetResponseInfo(info);
    }

    void SetResponseInfo(const char* format, ...)
    {
        Stroka info;
        va_list params;
        va_start(params, format);
        vsprintf(info, format, params);
        va_end(params);
        this->Context->SetResponseInfo(info);
    }

    Stroka GetResponseInfo()
    {
        return this->Context->GetResponseInfo();
    }

    using TBase::Wrap;

    // TODO(sandello): get rid of double binding here by delaying bind moment to the very last possible moment.
    TClosure Wrap(const TCallback<void(TIntrusivePtr<TThis>)>& paramAction)
    {
        YASSERT(!paramAction.IsNull());
        return this->Context->Wrap(BIND(paramAction, MakeStrong(this)));
    }

private:
    void SerializeResponseAndReply()
    {
        TSharedRef data;
        YCHECK(SerializeToProtoWithEnvelope(*Response_, &data, this->Options.ResponseCodec));
        this->Context->SetResponseBody(MoveRV(data));
        this->Context->Reply(TError());
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
        : TBase(MoveRV(context), options)
    { }

    using TBase::Wrap;

    TClosure Wrap(const TCallback<void(TIntrusivePtr<TThis>)>& paramAction)
    {
        YASSERT(paramAction);
        return this->Context->Wrap(BIND(paramAction, MakeStrong(this)));
    }
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
        //! If NULL then the default one is used.
        IInvokerPtr Invoker;

        //! Service method name.
        Stroka Verb;

        //! A handler that will serve the requests.
        THandler Handler;

        //! Is the method one-way?
        bool OneWay;

        //! Options to pass to the handler.
        THandlerInvocationOptions Options;

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

        TMethodDescriptor& SetResponseCodec(ECodecId value)
        {
            Options.ResponseCodec = value;
            return *this;
        }
    };

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TIntrinsicRefCounted
    {
        TRuntimeMethodInfo(
            const TMethodDescriptor& descriptor,
            const NYPath::TYPath& profilingPath);

        TMethodDescriptor Descriptor;

        //! Path prefix for all profiling information regarding this method.
        NYPath::TYPath ProfilingPath;

        //! Increments with each method call.
        NProfiling::TRateCounter RequestCounter;
    };

    typedef TIntrusivePtr<TRuntimeMethodInfo> TRuntimeMethodInfoPtr;

    //! A request that is currently being served.
    struct TActiveRequest
        : public TIntrinsicRefCounted
    {
        TActiveRequest(
            const TRequestId& id,
            NBus::IBusPtr replyBus,
            TRuntimeMethodInfoPtr runtimeInfo,
            const NProfiling::TTimer& timer);

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

        //! Measures various execution statistics.
        NProfiling::TTimer Timer;
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
        IInvokerPtr defaultInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    ~TServiceBase();

    //! Registers a method.
    void RegisterMethod(const TMethodDescriptor& descriptor);

    //! Prepares the handler to invocation.
    virtual TClosure PrepareHandler(IServiceContextPtr context, TClosure handler);

    //! Replies #error to every request in #ActiveRequests, clears the latter one.
    void CancelActiveRequests(const TError& error);

private:
    class TServiceContext;

    IInvokerPtr DefaultInvoker;
    Stroka ServiceName;
    Stroka LoggingCategory;
    NProfiling::TRateCounter RequestCounter;

    //! Protects #RuntimeMethodInfos and #ActiveRequests.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfoPtr> RuntimeMethodInfos;
    yhash_set<TActiveRequestPtr> ActiveRequests;

    virtual Stroka GetServiceName() const override;

    virtual void OnRequest(
        const NProto::TRequestHeader& header,
        NBus::IMessagePtr message,
        NBus::IBusPtr replyBus) override;

    void OnInvocationPrepared(
        TActiveRequestPtr activeRequest,
        IServiceContextPtr context,
        TClosure handler);

    void OnResponse(TActiveRequestPtr activeRequest, NBus::IMessagePtr message);

};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    TClosure method##Thunk( \
        ::NYT::NRpc::IServiceContextPtr context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = New<TCtx##method>(MoveRV(context), options); \
        typedContext->DeserializeRequest(); \
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
        BIND(&TThis::method##Thunk, Unretained(this)))

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_ONE_WAY_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayTypedServiceContext<ns::TReq##method> TCtx##method; \
    typedef TIntrusivePtr<TCtx##method> TCtx##method##Ptr; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    \
    TClosure method##Thunk( \
        ::NYT::NRpc::IServiceContextPtr context, \
        const ::NYT::NRpc::THandlerInvocationOptions& options) \
    { \
        auto typedContext = New<TCtx##method>(MoveRV(context), options); \
        typedContext->DeserializeRequest(); \
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
