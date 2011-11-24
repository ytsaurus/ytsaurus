#pragma once

#include "common.h"
#include "yson_events.h"

#include "../misc/property.h"
#include "../rpc/service.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EYPathErrorCode,
    ((ResolveError)(1))
    ((GenericError)(2))
    ((CommitError)(3))
);

////////////////////////////////////////////////////////////////////////////////

struct IYPathService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService> TPtr;

    class TResolveResult
    {
        DEFINE_BYVAL_RO_PROPERTY(IYPathService::TPtr, Service);
        DEFINE_BYVAL_RO_PROPERTY(TYPath, Path);

    public:
        static TResolveResult Here(TYPath path)
        {
            TResolveResult result;
            result.Path_ = path;
            return result;
        }

        static TResolveResult There(IYPathService* service, TYPath path)
        {
            TResolveResult result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        bool IsHere() const
        {
            return ~Service_ == NULL;
        }
    };

    virtual TResolveResult Resolve(TYPath path, const Stroka& verb) = 0;
    virtual void Invoke(NRpc::IServiceContext* context) = 0;

    static IYPathService::TPtr FromNode(INode* node);
    static IYPathService::TPtr FromProducer(TYsonProducer* producer);
};

typedef IFunc<NYTree::IYPathService::TPtr> TYPathServiceProvider;
typedef IFunc<TFuture<NYTree::IYPathService::TPtr>::TPtr> TYPathServiceAsyncProvider;

////////////////////////////////////////////////////////////////////////////////

struct IYPathExecutor
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathExecutor> TPtr;

    virtual void ExecuteVerb(
        IYPathService* service,
        NRpc::IServiceContext* context) = 0;
};

IYPathExecutor::TPtr GetDefaultExecutor();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
