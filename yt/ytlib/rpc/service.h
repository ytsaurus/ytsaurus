#pragma once

#include "client.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/hash.h>
#include <ytlib/misc/metric.h>
#include <ytlib/misc/error.h>
#include <ytlib/logging/log.h>

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
     *  The server should not rely on its uniqueness.
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

    //! Returns an immutable vector of request attachments.
    virtual const yvector<TSharedRef>& RequestAttachments() const = 0;
    //! Returns a mutable vector of response attachments.
    virtual yvector<TSharedRef>& ResponseAttachments() = 0;

    //! Sets and immediately logs the request logging info.
    virtual void SetRequestInfo(const Stroka& info) = 0;
    //! Returns the previously set request logging info.
    virtual Stroka GetRequestInfo() const = 0;

    //! Sets the response logging info. This info will be logged when the context is replied.
    virtual void SetResponseInfo(const Stroka& info) = 0;
    //! Returns the currently set response logging info.
    virtual Stroka GetResponseInfo() = 0;

    //! Wraps the given action into an exception guard that logs the exception and replies.
    virtual IAction::TPtr Wrap(IAction* action) = 0;
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

    const yvector<TSharedRef>& Attachments()
    {
        return Context->RequestAttachments();
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
        if (!DeserializeProtobuf(&Request_, Context->GetRequestBody())) {
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

    IAction::TPtr Wrap(IAction* action)
    {
        YASSERT(action);
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

    // NB: This overload is added to workaround VS2010 ICE inside lambdas calling Reply.
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
            if (!SerializeProtobuf(&Response_, &responseBlob)) {
                LOG_FATAL("Error serializing response");
            }
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

    IAction::TPtr Wrap(IParamAction<TPtr>* paramAction)
    {
        YASSERT(paramAction);
        return this->Context->Wrap(~paramAction->Bind(TPtr(this)));
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

    IAction::TPtr Wrap(IParamAction<TPtr>* paramAction)
    {
        YASSERT(paramAction);
        return this->Context->Wrap(~paramAction->Bind(TPtr(this)));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public IService
{
protected:
    //! Describes a handler for a service method.
    typedef IParamAction<IServiceContext*> THandler;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        //! Initializes the instance.
        TMethodDescriptor(
            const Stroka& verb,
            THandler* handler,
            bool oneWay = false)
            : Verb(verb)
            , Handler(handler)
            , OneWay(oneWay)
        {
            YASSERT(handler);
        }

        //! Service method name.
        Stroka Verb;

        //! A handler that will serve the requests.
        THandler::TPtr Handler;

        //! Is the method one-way?
        bool OneWay;
    };

    //! Initializes the instance.
    /*!
     *  \param defaultServiceInvoker
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
        IInvoker* defaultServiceInvoker,
        const Stroka& serviceName,
        const Stroka& loggingCategory);

    //! Registers a method.
    void RegisterMethod(const TMethodDescriptor& descriptor);

    //! Registers a method with a supplied custom invoker.
    void RegisterMethod(const TMethodDescriptor& descriptor, IInvoker* invoker);

private:
    struct TRuntimeMethodInfo
    {
        TRuntimeMethodInfo(const TMethodDescriptor& info, IInvoker* invoker)
            : Descriptor(info)
            , Invoker(invoker)
            // TODO: configure properly
            , ExecutionTime(0, 1000, 10)
        { }

        TMethodDescriptor Descriptor;
        IInvoker::TPtr Invoker;
        TMetric ExecutionTime;
    };

    struct TActiveRequest
    {
        TActiveRequest(
            TRuntimeMethodInfo* runtimeInfo,
            const TInstant& startTime)
            : RuntimeInfo(runtimeInfo)
            , StartTime(startTime)
        { }

        TRuntimeMethodInfo* RuntimeInfo;
        TInstant StartTime;
    };
    
    IInvoker::TPtr DefaultServiceInvoker;
    Stroka ServiceName;
    NLog::TLogger ServiceLogger;

    //! Protects #RuntimeMethodInfos and #ActiveRequests.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfo> RuntimeMethodInfos;
    yhash_map<IServiceContext::TPtr, TActiveRequest> ActiveRequests;

    virtual void OnBeginRequest(IServiceContext* context);
    virtual void OnEndRequest(IServiceContext* context);

    virtual Stroka GetLoggingCategory() const;
    virtual Stroka GetServiceName() const;

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
    TMethodDescriptor(#method, ~FromMethod(&TThis::method##Thunk, this), false)

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
    TMethodDescriptor(#method, ~FromMethod(&TThis::method##Thunk, this), true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
