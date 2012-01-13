#pragma once

#include "common.h"
#include "ytree_fwd.h"
#include "yson_consumer.h"

#include <ytlib/misc/property.h>
#include <ytlib/rpc/service.h>

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
        static TResolveResult Here(const TYPath& path)
        {
            TResolveResult result;
            result.Path_ = path;
            return result;
        }

        static TResolveResult There(IYPathService* service, const TYPath& path)
        {
            YASSERT(service);

            TResolveResult result;
            result.Service_ = service;
            result.Path_ = path;
            return result;
        }

        bool IsHere() const
        {
            return !Service_;
        }
    };

    virtual TResolveResult Resolve(const TYPath& path, const Stroka& verb) = 0;
    virtual void Invoke(NRpc::IServiceContext* context) = 0;

    static IYPathService::TPtr FromProducer(TYsonProducer* producer);
};

typedef IFunc<NYTree::IYPathService::TPtr> TYPathServiceProvider;

////////////////////////////////////////////////////////////////////////////////

struct IYPathProcessor
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathProcessor> TPtr;

    virtual void Resolve(
        const TYPath& path,
        const Stroka& verb,
        IYPathService::TPtr* suffixService,
        TYPath* suffixPath) = 0;

    virtual void Execute(
        IYPathService* service,
        NRpc::IServiceContext* context) = 0;
};

IYPathProcessor::TPtr CreateDefaultProcessor(IYPathService* rootService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
