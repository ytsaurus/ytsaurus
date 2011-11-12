#pragma once

#include "common.h"
#include "yson_events.h"

#include "../misc/property.h"
#include "../rpc/service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_POLY_ENUM2(EYPathErrorCode, NRpc::EErrorCode,
    ((GenericError)(100))
);

////////////////////////////////////////////////////////////////////////////////

struct IYPathService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService> TPtr;

    class TNavigateResult
    {
        DECLARE_BYVAL_RO_PROPERTY(Service, IYPathService::TPtr);
        DECLARE_BYVAL_RO_PROPERTY(Path, TYPath);

    public:
        static TNavigateResult Here()
        {
            return TNavigateResult();
        }

        static TNavigateResult There(IYPathService* service, TYPath path)
        {
            TNavigateResult result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        bool IsHere() const
        {
            return ~Service_ == NULL;
        }
    };

    virtual TNavigateResult Navigate(TYPath path, bool mustExist) = 0;

    virtual void Invoke(NRpc::IServiceContext* context) = 0;

    static IYPathService::TPtr FromNode(INode* node);
    static IYPathService::TPtr FromProducer(TYsonProducer* producer);
};

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath);

////////////////////////////////////////////////////////////////////////////////

void NavigateYPath(
    IYPathService* rootService,
    TYPath path,
    bool mustExist,
    IYPathService::TPtr* tailService,
    TYPath* tailPath);

IYPathService::TPtr NavigateYPath(
    IYPathService* rootService,
    TYPath path);

////////////////////////////////////////////////////////////////////////////////

struct TYPathResponseHandlerParam
{
    NRpc::TError Error;
    NBus::IMessage::TPtr Message;
};

typedef IParamAction<const TYPathResponseHandlerParam&> TYPathResponseHandler;

void InvokeYPathVerb(
    IYPathService* service,
    TYPath path,
    const Stroka& verb,
    NRpc::IServiceContext* outerContext,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

NRpc::IServiceContext::TPtr UnwrapYPathRequest(
    NRpc::IServiceContext* outerContext,
    TYPath path,
    const Stroka& verb,
    const Stroka& loggingCategory,
    TYPathResponseHandler* responseHandler);

void WrapYPathResponse(
    NRpc::IServiceContext* outerContext,
    NBus::IMessage* responseMessage);

//INode::TPtr NavigateYPath(
//    IYPathService::TPtr rootService,
//    TYPath path);
//
//void GetYPath(
//    IYPathService::TPtr rootService,
//    TYPath path,
//    IYsonConsumer* consumer);
//
//INode::TPtr SetYPath(
//    IYPathService::TPtr rootService,
//    TYPath path,
//    TYsonProducer::TPtr producer);
//
//void RemoveYPath(
//    IYPathService::TPtr rootService,
//    TYPath path);
//
//void LockYPath(
//    IYPathService::TPtr rootService,
//    TYPath path);
//

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
