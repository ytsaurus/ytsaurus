#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

#include "../misc/property.h"
#include "../rpc/service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYPathService2
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService2> TPtr;

    class TNavigateResult2
    {
        DECLARE_BYVAL_RO_PROPERTY(Service, IYPathService2::TPtr);
        DECLARE_BYVAL_RO_PROPERTY(Path, TYPath);

    public:
        static TNavigateResult2 Here()
        {
            return TNavigateResult2();
        }

        static TNavigateResult2 There(IYPathService2* service, TYPath path)
        {
            TNavigateResult2 result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        bool IsHere() const
        {
            return ~Service_ == NULL;
        }
    };

    virtual TNavigateResult2 Navigate2(TYPath path) = 0;

    virtual void Invoke2(TYPath path, NRpc::TServiceContext* context) = 0;


    static IYPathService2::TPtr FromNode(INode* node);
};


#define YPATH_SERVICE_METHOD_DECL(ns, method) \
    typedef ::NYT::NRpc::TTypedServiceRequest<ns::TReq##method, ns::TRsp##method> TReq##method; \
    typedef ::NYT::NRpc::TTypedServiceResponse<ns::TReq##method, ns::TRsp##method> TRsp##method; \
    typedef ::NYT::NRpc::TTypedServiceContext<ns::TReq##method, ns::TRsp##method> TCtx##method; \
    \
    void method##Thunk(TYPath path, ::NYT::NRpc::TServiceContext::TPtr context) \
    { \
        auto typedContext = New<TCtx##method>(context); \
        method( \
            path, \
            &typedContext->Request(), \
            &typedContext->Response(), \
            typedContext); \
    } \
    \
    void method( \
        TYPath path, \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method::TPtr context)

#define YPATH_SERVICE_METHOD_IMPL(type, method) \
    void type::method( \
        TYPath path, \
        TReq##method* request, \
        TRsp##method* response, \
        TCtx##method::TPtr context)


struct IYPathService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService> TPtr;

    static IYPathService::TPtr FromNode(INode* node);
    static IYPathService::TPtr FromProducer(TYsonProducer* producer);

    DECLARE_ENUM(ECode,
        (Undefined)
        (Done)
        (Recurse)
    );

    template <class T>
    struct TResult
    {
        TResult()
            : Code(ECode::Undefined)
        { }

        template <class TOther>
        TResult(const TResult<TOther>& other)
        {
            YASSERT(other.Code == ECode::Recurse);
            Code = ECode::Recurse;
            RecurseService = other.RecurseService;
            RecursePath = other.RecursePath;
        }

        ECode Code;
        
        // Done
        T Value;

        // Recurse
        IYPathService::TPtr RecurseService;
        TYPath RecursePath;
        
        static TResult CreateDone(const T& value = T())
        {
            TResult result;
            result.Code = ECode::Done;
            result.Value = value;
            return result;
        }

        static TResult CreateRecurse(
            IYPathService::TPtr recurseService,
            TYPath recursePath)
        {
            TResult result;
            result.Code = ECode::Recurse;
            result.RecurseService = recurseService;
            result.RecursePath = recursePath;
            return result;
        }
    };

    typedef TResult<INode::TPtr> TNavigateResult;
    virtual TNavigateResult Navigate(TYPath path) = 0;

    typedef TResult<TVoid> TGetResult;
    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer) = 0;

    typedef TResult<INode::TPtr> TSetResult;
    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer) = 0;

    typedef TResult<TVoid> TRemoveResult;
    virtual TRemoveResult Remove(TYPath path) = 0;

    typedef TResult<TVoid> TLockResult;
    virtual TLockResult Lock(TYPath path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath);

INode::TPtr NavigateYPath(
    IYPathService::TPtr rootService,
    TYPath path);

void GetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    IYsonConsumer* consumer);

INode::TPtr SetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    TYsonProducer::TPtr producer);

void RemoveYPath(
    IYPathService::TPtr rootService,
    TYPath path);

void LockYPath(
    IYPathService::TPtr rootService,
    TYPath path);

////////////////////////////////////////////////////////////////////////////////

class TYTreeException : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
