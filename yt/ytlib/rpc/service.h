#pragma once

#include "common.h"
#include "client.h"
#include "message.h"

#include "../misc/property.h"
#include "../misc/hash.h"
#include "../misc/metric.h"
#include "../logging/log.h"

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
    explicit TServiceException(EErrorCode errorCode = EErrorCode::ServiceError)
        : ErrorCode(errorCode)
    { }

    //! Gets the error code.
    TError GetError() const
    {
        return TError(ErrorCode, what());
    }

protected:
    EErrorCode ErrorCode;

};

////////////////////////////////////////////////////////////////////////////////

//! A typed version of TServiceException.
/*!
 *  The primary difference from the untyped TServiceException is that the
 *  constructor accepts an error of a given TErrorCode type.
 *  
 *  This enables to capture the error message during exception construction
 *  and write
 *  \code
 *  typedef TTypedServiceException<EMyCode> TMyException;
 *  ythrow TMyException(EMyCode::SomethingWrong);
 *  \endcode
 *  instead of
 *  \code
 *  ythrow TServiceException(EMyCode(EMyCode::SomethingWrong));
 *  \endcode
 */
template <class TErrorCode>
class TTypedServiceException 
    : public TServiceException
{
public:
    //! Initializes a new instance.
    explicit TTypedServiceException(TErrorCode errorCode = EErrorCode::ServiceError)
        : TServiceException(errorCode)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct IServiceContext
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IServiceContext> TPtr;

    virtual NBus::IMessage::TPtr GetRequestMessage() const = 0;

    virtual Stroka GetPath() const = 0;
    virtual Stroka GetVerb() const = 0;

    virtual void Reply(const TError& error) = 0;
    virtual bool IsReplied() const = 0;

    virtual TSharedRef GetRequestBody() const = 0;
    virtual void SetResponseBody(TBlob&& responseBody) = 0;

    virtual const yvector<TSharedRef>& RequestAttachments() const = 0;
    virtual yvector<TSharedRef>& ResponseAttachments() = 0;

    virtual void SetRequestInfo(const Stroka& info) = 0;
    virtual Stroka GetRequestInfo() const = 0;

    virtual void SetResponseInfo(const Stroka& info) = 0;
    virtual Stroka GetResponseInfo() = 0;

    virtual IAction::TPtr Wrap(IAction* action) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IService> TPtr;

    virtual Stroka GetServiceName() const = 0;
    virtual Stroka GetLoggingCategory() const = 0;

    virtual void OnBeginRequest(IServiceContext* context) = 0;
    virtual void OnEndRequest(IServiceContext* context) = 0;

    virtual Stroka GetDebugInfo() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

template<class TRequestMessage, class TResponseMessage>
class TTypedServiceRequest
    : public TRequestMessage
    , private TNonCopyable
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

template<class TRequestMessage, class TResponseMessage>
class TTypedServiceResponse
    : public TResponseMessage
    , private TNonCopyable
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

// TODO: move impl to inl?
template<class TRequestMesssage, class TResponseMessage>
class TTypedServiceContext
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr< TTypedServiceContext<TRequestMesssage, TResponseMessage> > TPtr;
    typedef TTypedServiceRequest<TRequestMesssage, TResponseMessage> TTypedRequest;
    typedef TTypedServiceResponse<TRequestMesssage, TResponseMessage> TTypedResponse;

    DECLARE_BYREF_RW_PROPERTY(Request, TTypedRequest);
    DECLARE_BYREF_RW_PROPERTY(Response, TTypedResponse);

public:
    TTypedServiceContext(IServiceContext* context)
        : Logger(RpcLogger)
        , Context(context)
        , Request_(context)
        , Response_(context)
    {
        YASSERT(context != NULL);

        if (!DeserializeMessage(&Request_, context->GetRequestBody())) {
            ythrow TServiceException(EErrorCode::ProtocolError) <<
                "Error deserializing request body";
        }
    }

    Stroka GetPath() const
    {
        return Context->GetPath();
    }

    Stroka GetVerb() const
    {
        return Context->GetVerb();
    }

    // NB: This overload is added to workaround VS2010 ICE inside lambdas calling Reply.
    void Reply()
    {
        Reply(EErrorCode::OK);
    }

    void Reply(EErrorCode errorCode)
    {
        Reply(TError(errorCode));
    }

    void Reply(const TError& error)
    {
        if (error.IsOK()) {
            TBlob responseBlob;
            if (!SerializeMessage(&Response_, &responseBlob)) {
                LOG_FATAL("Error serializing response");
            }
            Context->SetResponseBody(MoveRV(responseBlob));
        }
        Context->Reply(error);
    }

    bool IsReplied() const
    {
        return Context->IsReplied();
    }

    IAction::TPtr Wrap(IParamAction<TPtr>* paramAction)
    {
        YASSERT(paramAction != NULL);
        return Context->Wrap(~paramAction->Bind(TPtr(this)));
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

    void SetResponseInfo(const Stroka& info)
    {
        Context->SetResponseInfo(info);
    }

    void SetResponseInfo(const char* format, ...)
    {
        Stroka info;
        va_list params;
        va_start(params, format);
        vsprintf(info, format, params);
        va_end(params);
        Context->SetResponseInfo(info);
    }

    Stroka GetResponseInfo()
    {
        return Context->GetResponseInfo();
    }

    IServiceContext::TPtr GetUntypedContext() const
    {
        return Context;
    }

private:
    NLog::TLogger& Logger;
    IServiceContext::TPtr Context;

};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for implementing IService.
class TServiceBase
    : public IService
{
public:
    //! Reports debug info of the running service instance.
    Stroka GetDebugInfo() const;

protected:
    //! Describes a handler for a service method.
    typedef IParamAction<IServiceContext*> THandler;

    //! Information needed to a register a service method.
    struct TMethodDescriptor
    {
        //! Initializes the instance.
        TMethodDescriptor(const Stroka& verb, THandler* handler)
            : Verb(verb)
            , Handler(handler)
        {
            YASSERT(handler != NULL);
        }

        //! Service method name.
        Stroka Verb;
        //! A handler that will serve the requests.
        THandler::TPtr Handler;
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
        TMethodDescriptor Descriptor;
        IInvoker::TPtr Invoker;
        TMetric ExecutionTime;

        TRuntimeMethodInfo(const TMethodDescriptor& info, IInvoker* invoker)
            : Descriptor(info)
            , Invoker(invoker)
            // TODO: configure properly
            , ExecutionTime(0, 1000, 10)
        { }
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

    //! Protects #RuntimeMethodInfos and #OutstandingRequests.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfo> RuntimeMethodInfos;
    yhash_map<IServiceContext::TPtr, TActiveRequest> ActiveRequests;

    virtual void OnBeginRequest(IServiceContext* context);
    virtual void OnEndRequest(IServiceContext* context);

    virtual Stroka GetLoggingCategory() const;
    virtual Stroka GetServiceName() const;

};

////////////////////////////////////////////////////////////////////////////////

#define RPC_SERVICE_METHOD_DECL(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NRpc::TTypedServiceResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    \
    void method##Thunk(::NYT::NRpc::IServiceContext* context) \
    { \
        auto typedContext = New<TCtx##method>(context); \
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

#define RPC_SERVICE_METHOD_IMPL(type, method) \
    void type::method( \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method::TPtr context)

#define RPC_SERVICE_METHOD_DESC(method) \
    TMethodDescriptor(#method, ~FromMethod(&TThis::method##Thunk, this)) \

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
