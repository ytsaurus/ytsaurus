#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral_node_factory.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "convert.h"

#include <core/rpc/dispatcher.h>

#include <core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TFromProducerYPathService
    : public TYPathServiceBase
    , public TSupportsGet
{
public:
    explicit TFromProducerYPathService(TYsonProducer producer)
        : Producer(producer)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr context) override
    {
        // Try to handle root get requests without constructing ephemeral YTree.
        if (path.empty() && context->GetMethod() == "Get") {
            return TResolveResult::Here(path);
        } else {
            auto node = BuildNodeFromProducer();
            return TResolveResult::There(node, path);
        }
    }

private:
    TYsonProducer Producer;

    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return TYPathServiceBase::DoInvoke(context);
    }

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override
    {
        if (!request->ignore_opaque() ||
            request->attribute_filter().mode() != EAttributeFilterMode::All)
        {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, IServiceContextPtr(context));
            return;
        }

        Stroka result;
        TStringOutput stream(result);
        TYsonWriter writer(&stream, EYsonFormat::Binary, EYsonType::Node, true);
        Producer.Run(&writer);

        response->set_value(result);
        context->Reply();
    }

    virtual void GetRecursive(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGetPtr context) override
    {
        YUNREACHABLE();
    }

    virtual void GetAttribute(const TYPath& path, TReqGet* request, TRspGet* response, TCtxGetPtr context) override
    {
        YUNREACHABLE();
    }


    INodePtr BuildNodeFromProducer()
    {
        return ConvertTo<INodePtr>(Producer);
    }

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
        if (ExpirationPeriod != TDuration::Zero()) {
            UpdateCache(ConvertToNode(TYsonString("#")));
            GetCachedTree(true);
        }
    }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        if (ExpirationPeriod == TDuration::Zero()) {
            // Cache disabled.
            return TResolveResult::There(UnderlyingService, path);
        }

        return TResolveResult::There(GetCachedTree(false), path);
    }

private:
    IYPathServicePtr UnderlyingService;
    TDuration ExpirationPeriod;

    TSpinLock SpinLock;
    INodePtr CachedTree;
    TInstant LastUpdateTime;
    bool Updating = false;

    virtual bool DoInvoke(IServiceContextPtr /*context*/) override
    {
        YUNREACHABLE();
    }


    INodePtr GetCachedTree(bool forceUpdate)
    {
        bool needsUpdate = false;
        INodePtr cachedTree;
        {
            TGuard<TSpinLock> guard(SpinLock);
            cachedTree = CachedTree;
            if ((forceUpdate || TInstant::Now() > LastUpdateTime + ExpirationPeriod) && !Updating) {
                needsUpdate = true;
                Updating = true;
            }
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
        return AsyncYPathGet(
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
        Updating = false;
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
