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
        : Producer_(producer)
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
    TYsonProducer Producer_;

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
        Producer_.Run(&writer);

        response->set_value(result);
        context->Reply();
    }

    virtual void GetRecursive(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, TCtxGetPtr /*context*/) override
    {
        YUNREACHABLE();
    }

    virtual void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, TCtxGetPtr /*context*/) override
    {
        YUNREACHABLE();
    }


    INodePtr BuildNodeFromProducer()
    {
        return ConvertTo<INodePtr>(Producer_);
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
        : UnderlyingService_(underlyingService)
        , Invoker_(invoker)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        return TResolveResult::Here(path);
    }

private:
    IYPathServicePtr UnderlyingService_;
    IInvokerPtr Invoker_;

    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        auto this_ = MakeStrong(this);
        Invoker_->Invoke(BIND([this, this_, context] () {
            ExecuteVerb(UnderlyingService_, context);
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
        TDuration expirationTime)
        : UnderlyingService_(underlyingService)
        , ExpirationTime_(expirationTime)
    { }
    
    virtual TResolveResult Resolve(const TYPath& path, IServiceContextPtr /*context*/) override
    {
        if (ExpirationTime_ == TDuration::Zero()) {
            return TResolveResult::There(UnderlyingService_, path);
        } else {
            return TResolveResult::Here(path);
        }
    }

private:
    IYPathServicePtr UnderlyingService_;
    TDuration ExpirationTime_;

    TSpinLock SpinLock_;
    TFuture<INodePtr> CachedTree_;
    TInstant LastUpdateTime_;


    virtual bool DoInvoke(IServiceContextPtr context) override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        
        if (!CachedTree_ ||
            (CachedTree_.IsSet() && LastUpdateTime_ + ExpirationTime_ < TInstant::Now()))
        {
            auto promise = NewPromise<INodePtr>();
            CachedTree_ = promise;
            guard.Release();

            AsyncYPathGet(
                UnderlyingService_,
                "",
                TAttributeFilter::All,
                true)
                .Subscribe(BIND(&TCachedYPathService::OnGotTree, MakeStrong(this), promise)
                    // Nothing to be proud of, but we do need some large pool.
                    .Via(NRpc::TDispatcher::Get()->GetPoolInvoker()));
        }

        CachedTree_.Subscribe(BIND([=] (INodePtr root) {
            ExecuteVerb(root, context);
        }));

        return true;
    }

    void OnGotTree(TPromise<INodePtr> promise, TErrorOr<TYsonString> result)
    {
        YCHECK(result.IsOK());

        {
            TGuard<TSpinLock> guard(SpinLock_);
            LastUpdateTime_ = TInstant::Now();
        }

        promise.Set(ConvertToNode(result.Value()));
    }

};

IYPathServicePtr IYPathService::Cached(TDuration expirationTime)
{
    return New<TCachedYPathService>(this, expirationTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
