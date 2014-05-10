#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral_node_factory.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "convert.h"

#include <core/rpc/dispatcher.h>

namespace NYT {
namespace NYTree {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TFromProducerYPathService
    : public TYPathServiceBase
{
public:
    explicit TFromProducerYPathService(TYsonProducer producer)
        : Producer(producer)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        auto builder = CreateBuilderFromFactory(GetEphemeralNodeFactory());
        builder->BeginTree();
        Producer.Run(builder.get());
        auto node = builder->EndTree();
        return TResolveResult::There(node, path);
    }

private:
    TYsonProducer Producer;

};

IYPathServicePtr IYPathService::FromProducer(TYsonProducer producer)
{
    return New<TFromProducerYPathService>(producer);
}

////////////////////////////////////////////////////////////////////////////////

class TViaYPathService
    : public TYPathServiceBase
{
public:
    TViaYPathService(
        IYPathServicePtr underlyingService,
        IInvokerPtr invoker)
        : UnderlyingService(underlyingService)
        , Invoker(invoker)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        return TResolveResult::Here(path);
    }

private:
    IYPathServicePtr UnderlyingService;
    IInvokerPtr Invoker;

    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        auto this_ = MakeStrong(this);
        Invoker->Invoke(BIND([this, this_, context] () {
            try {
                ExecuteVerb(UnderlyingService, context);
            } catch (const std::exception& ex) {
                context->Reply(ex);
            }
        }));
        return true;
    }
};

IYPathServicePtr IYPathService::Via(IInvokerPtr invoker)
{
    return New<TViaYPathService>(this, invoker);
}

////////////////////////////////////////////////////////////////////////////////

class TCachedYPathService
    : public TYPathServiceBase
{
public:
    TCachedYPathService(
        IYPathServicePtr underlyingService,
        TDuration expirationPeriod)
        : UnderlyingService(underlyingService)
        , ExpirationPeriod(expirationPeriod)
    { }
    
    void Initialize()
    {
        auto tree = ConvertToNode(AsyncGetData().Get());
        UpdateCache(tree);
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        if (ExpirationPeriod == TDuration::Zero()) {
            // Cache disabled.
            return TResolveResult::There(UnderlyingService, path);
        }

        return TResolveResult::There(GetCachedTree(), path);
    }

private:
    IYPathServicePtr UnderlyingService;
    TDuration ExpirationPeriod;

    TSpinLock SpinLock;
    INodePtr CachedTree;
    TInstant LastUpdateTime;


    virtual bool DoInvoke(IServiceContextPtr /*context*/) override
    {
        YUNREACHABLE();
    }


    INodePtr GetCachedTree()
    {
        bool needsUpdate;
        INodePtr cachedTree;
        {
            TGuard<TSpinLock> guard(SpinLock);
            needsUpdate = TInstant::Now() > LastUpdateTime + ExpirationPeriod;
            cachedTree = CachedTree;
        }

        if (needsUpdate) {
            AsyncGetData()
                .Apply(
                    BIND([] (TYsonString result) {
                        return ConvertToNode(result);
                    })
                    // Nothing to be proud of, but we do need some large pool.
                    .AsyncVia(NRpc::TDispatcher::Get()->GetPoolInvoker()))
                .Subscribe(
                    BIND(&TCachedYPathService::UpdateCache, MakeStrong(this)));
        }

        return cachedTree;
    }

    TFuture<TYsonString> AsyncGetData()
    {
        return
            AsyncYPathGet(
                UnderlyingService,
                "",
                TAttributeFilter::All,
                true)
                .Apply(BIND([] (TErrorOr<TYsonString> result) -> TYsonString {
                    YCHECK(result.IsOK());
                    return result.Value();
                }));
    }

    void UpdateCache(INodePtr tree)
    {
        TGuard<TSpinLock> guard(SpinLock);
        CachedTree = tree;
        LastUpdateTime = TInstant::Now();
    }

};

IYPathServicePtr IYPathService::Cached(TDuration expirationPeriod)
{
    auto result = New<TCachedYPathService>(this, expirationPeriod);
    result->Initialize();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
