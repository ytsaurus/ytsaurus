#include "stdafx.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ephemeral_node_factory.h"
#include "ypath_client.h"
#include "ypath_detail.h"
#include "convert.h"

#include <core/rpc/dispatcher.h>

#include <core/yson/async_consumer.h>
#include <core/yson/attribute_consumer.h>
#include <core/yson/writer.h>

namespace NYT {
namespace NYTree {

using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter TAttributeFilter::All(EAttributeFilterMode::All, std::vector<Stroka>());
TAttributeFilter TAttributeFilter::None(EAttributeFilterMode::None, std::vector<Stroka>());

TAttributeFilter::TAttributeFilter()
    : Mode(EAttributeFilterMode::None)
{ }

TAttributeFilter::TAttributeFilter(
    EAttributeFilterMode mode,
    const std::vector<Stroka>& keys)
    : Mode(mode)
      , Keys(keys)
{ }

TAttributeFilter::TAttributeFilter(EAttributeFilterMode mode)
    : Mode(mode)
{ }

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter)
{
    protoFilter->set_mode(static_cast<int>(filter.Mode));
    for (const auto& key : filter.Keys) {
        protoFilter->add_keys(key);
    }
}

void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter)
{
    *filter = TAttributeFilter(
        EAttributeFilterMode(protoFilter.mode()),
        NYT::FromProto<Stroka>(protoFilter.keys()));
}

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
        bool ignoreOpaque = request->ignore_opaque();
        auto mode = EAttributeFilterMode(request->attribute_filter().mode());
        if (!ignoreOpaque || mode != EAttributeFilterMode::All)  {
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
        Invoker_->Invoke(BIND([=, this_ = MakeStrong(this)] () {
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
                    .Via(NRpc::TDispatcher::Get()->GetInvoker()));
        }

        CachedTree_.Subscribe(BIND([=] (const TErrorOr<INodePtr>& rootOrError) {
            if (rootOrError.IsOK()) {
                ExecuteVerb(rootOrError.Value(), context);
            }
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

void IYPathService::WriteAttributes(
    IAsyncYsonConsumer* consumer,
    const TAttributeFilter& filter,
    bool sortKeys)
{
    if (filter.Mode == EAttributeFilterMode::None)
        return;

    if (filter.Mode == EAttributeFilterMode::MatchingOnly && filter.Keys.empty())
        return;

    TAttributeFragmentConsumer attributesConsumer(consumer);
    WriteAttributesFragment(&attributesConsumer, filter, sortKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
