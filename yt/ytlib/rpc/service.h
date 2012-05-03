#pragma once

#include "public.h"
#include "client.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/hash.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/error.h>
#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

#include <util/generic/yexception.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Represents an error that has occurred during serving an RPC request.
class TServiceException 
    : public yexception
{
public:
    //! Initializes a new instance.
    explicit TServiceException(int code)
        : Code_(code)
    { }

    explicit TServiceException(const TError& error)
        : Code_(error.GetCode())
    {
        *this << error.ToString();
    }

    //! Gets the error code.
    TError GetError() const
    {
        return TError(Code_, what());
    }

protected:
    int Code_;

};

////////////////////////////////////////////////////////////////////////////////

//! Represents an RPC request at server-side.
struct IServiceContext
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IServiceContext> TPtr;

    //! Returns the message that contains the request being handled.
    virtual NBus::IMessage::TPtr GetRequestMessage() const = 0;

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

    //! An extension method that extracts the error code, the response body, and attachments
    //! from #message and replies to the client.
    void Reply(NBus::IMessage* message);

    //! Returns the error that was previously set by #Reply.
    /*!
     *  Calling #GetError before #Reply is forbidden.
     */
    virtual TError GetError() const = 0;

    //! Returns the request body.
    virtual TSharedRef GetRequestBody() const = 0;

    //! Sets the response body.
    virtual void SetResponseBody(const TSharedRef& responseBody) = 0;

    //! Returns a vector of request attachments.
    virtual yvector<TSharedRef>& RequestAttachments() = 0;

    //! Returns request attributes.
    virtual NYTree::IAttributeDictionary& RequestAttributes() = 0;

    //! Returns a vector of response attachments.
    virtual yvector<TSharedRef>& ResponseAttachments() = 0;

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
    virtual TClosure Wrap(TClosure action) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<IService> TPtr;

    virtual Stroka GetServiceName() const = 0;
    virtual Stroka GetLoggingCategory() const = 0;

    virtual void OnBeginRequest(IServiceContext* context) = 0;
    virtual void OnEndRequest(IServiceContext* context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage>
class TTypedServiceRequest
    : public TRequestMessage
{
public:
    TTypedServiceRequest(IServiceContext* context)
        : Context(context)
    { }

    yvector<TSharedRef>& Attachments()
    {
        return Context->RequestAttachments();
    }

    NYTree::IAttributeDictionary& Attributes() const
    {
        return Context->RequestAttributes();
    }

private:
    IServiceContext::TPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
{
public:
    TTypedServiceResponse(IServiceContext* context)
        : Context(context)
    { }

    yvector<TSharedRef>& Attachments()
    {
        return Context->ResponseAttachments();
    }

    NYTree::IAttributeDictionary& Attributes()
    {
        return Context->ResponseAttributes();
    }

private:
    IServiceContext::TPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for both one-way and two-way contexts.
template <class TRequestMessage>
class TTypedServiceContextBase
    : public TRefCounted
{
public:
    typedef TTypedServiceContextBase<TRequestMessage> TThis;
    typedef TIntrusivePtr<TThis> TPtr;
    typedef TTypedServiceRequest<TRequestMessage> TTypedRequest;

    DEFINE_BYREF_RW_PROPERTY(TTypedRequest, Request);

public:
    TTypedServiceContextBase(IServiceContext* context)
        : Request_(context)
        , Logger(RpcLogger)
        , Context(context)
    {
        YASSERT(context);

    }

    void Deserialize()
    {
        if (!DeserializeFromProto(&Request_, Context->GetRequestBody())) {
            ythrow TServiceException(EErrorCode::ProtocolError) <<
                "Error deserializing request body";
        }
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

    IServiceContext::TPtr GetUntypedContext() const
    {
        return Context;
    }

    TClosure Wrap(TClosure action)
    {
        YASSERT(!action.IsNull());
        return Context->Wrap(action);
    }

protected:
    NLog::TLogger& Logger;
    IServiceContext::TPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

//! Describes a two-way context.
template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext
    : public TTypedServiceContextBase<TRequestMessage>
{
public:
    typedef TTypedServiceContext<TRequestMessage, TResponseMessage> TThis;
    typedef TTypedServiceContextBase<TRequestMessage> TBase;
    typedef TIntrusivePtr<TThis> TPtr;
    typedef TTypedServiceResponse<TResponseMessage> TTypedResponse;

    DEFINE_BYREF_RW_PROPERTY(TTypedResponse, Response);

public:
    TTypedServiceContext(IServiceContext* context)
        : TBase(context)
        , Response_(context)
    { }

    // XXX(sandello): If you ever change signature of any of Reply() functions,
    // please, search sources for "(*Context::*)(int, const Stroka&)" casts.
    // These casts mainly used to explicitly choose overloaded function when
    // binding it to some callback.

    void Reply()
    {
        Reply(TError(NYT::TError::OK, ""));
    }

    void Reply(int code, const Stroka& message)
    {
        Reply(TError(code, message));
    }

    void Reply(const TError& error)
    {
        NLog::TLogger& Logger = RpcLogger;
        if (error.IsOK()) {
            TBlob responseBlob;
            YVERIFY(SerializeToProto(&Response_, &responseBlob));
            this->Context->SetResponseBody(MoveRV(responseBlob));
        }
        this->Context->Reply(error);
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
    TClosure Wrap(TCallback<void(TPtr)> paramAction)
    {
        YASSERT(!paramAction.IsNull());
        return this->Context->Wrap(BIND(paramAction, MakeStrong(this)));
    }

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
    typedef TIntrusivePtr<TThis> TPtr;

public:
    TOneWayTypedServiceContext(IServiceContext* context)
        : TBase(context)
    { }

    using TBase::Wrap;

    TClosure Wrap(TCallback<void(TPtr)> paramAction)
    {
        YASSERT(paramAction);
        return this->Context->Wrap(~paramAction->BIND(MakeStrong(this)));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public IService
{
protected:
    //! Describes a handler for a service method.
    typedef TCallback<void(IServiceContext*)> THandler;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        //! Initializes the instance.
        TMethodDescriptor(
            const Stroka& verb,
            THandler handler,
            bool oneWay = false)
            : Verb(verb)
            , Handler(handler)
            , OneWay(oneWay)
        { }

        //! Service method name.
        Stroka Verb;

        //! A handler that will serve the requests.
        THandler Handler;

        //! Is the method one-way?
        bool OneWay;
    };

    //! Describes a service method and its runtime statistics.
    struct TRuntimeMethodInfo
        : public TIntrinsicRefCounted
    {
        TRuntimeMethodInfo(
            const TMethodDescriptor& descriptor,
            IInvoker* invoker,
            const NYTree::TYPath& profilingPath);

        TMethodDescriptor Descriptor;
        //! Invoker that is used to handle all requests for this method.
        IInvoker::TPtr Invoker;
        //! Path prefix for all profiling information regarding this method.
        NYTree::TYPath ProfilingPath;
        //! Increments with each method call.
        NProfiling::TRateCounter RequestCounter;
    };

    typedef TIntrusivePtr<TRuntimeMethodInfo> TRuntimeMethodInfoPtr;

    //! A request that is currently being served.
    struct TActiveRequest
        : public TIntrinsicRefCounted
    {
        TActiveRequest(
            TRuntimeMethodInfoPtr runtimeInfo,
            const NProfiling::TTimer& timer)
            : RuntimeInfo(runtimeInfo)
            , Timer(timer)
            , RunningSync(false)
            , Completed(false)
        { }

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
        IInvoker* defaultInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    ~TServiceBase();

    //! Registers a method.
    void RegisterMethod(const TMethodDescriptor& descriptor);

    //! Registers a method with a supplied custom invoker.
    void RegisterMethod(const TMethodDescriptor& descriptor, IInvoker* invoker);

private:
    IInvoker::TPtr DefaultInvoker;
    Stroka ServiceName;
    Stroka LoggingCategory;
    NProfiling::TRateCounter RequestCounter;

    //! Protects #RuntimeMethodInfos and #ActiveRequests.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfoPtr> RuntimeMethodInfos;
    yhash_map<IServiceContext::TPtr, TActiveRequestPtr> ActiveRequests;

    virtual Stroka GetServiceName() const;
    virtual Stroka GetLoggingCategory() const;

    virtual void OnBeginRequest(IServiceContext* context);
    virtual void OnEndRequest(IServiceContext* context);

    virtual void InvokeHandler(
        TRuntimeMethodInfo* runtimeInfo,
        const TClosure& handler,
        IServiceContext* context);
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    typedef TCtx##method::TTypedResponse TRsp##method; \
    \
    void method##Thunk(::NYT::NRpc::IServiceContext* context) \
    { \
        auto typedContext = New<TCtx##method>(context); \
        typedContext->Deserialize(); \
        method( \
            &typedContext->Request(), \
            &typedContext->Response(), \
            typedContext); \
    } \
    \
    void method( \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method::TPtr context)

#define DEFINE_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method::TPtr context)

#define RPC_SERVICE_METHOD_DESC(method) \
    ::NYT::NRpc::TServiceBase::TMethodDescriptor( \
        #method, \
        BIND(&TThis::method##Thunk, Unretained(this)), \
        false)

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_ONE_WAY_RPC_SERVICE_METHOD(ns, method) \
    typedef ::NYT::NRpc::TOneWayTypedServiceContext<ns::TReq##method> TCtx##method; \
    typedef TCtx##method::TTypedRequest  TReq##method; \
    \
    void method##Thunk(::NYT::NRpc::IServiceContext* context) \
    { \
        auto typedContext = New<TCtx##method>(context); \
        typedContext->Deserialize(); \
        method( \
            &typedContext->Request(), \
            typedContext); \
    } \
    \
    void method( \
        TReq##method* request, \
        TCtx##method::TPtr context)

#define DEFINE_ONE_WAY_RPC_SERVICE_METHOD(type, method) \
    void type::method( \
        TReq##method* request, \
        TCtx##method::TPtr context)

#define ONE_WAY_RPC_SERVICE_METHOD_DESC(method) \
    TMethodDescriptor( \
        #method, \
        BIND(&TThis::method##Thunk, Unretained(this)), \
        true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
