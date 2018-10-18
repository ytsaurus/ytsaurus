#include "ypath_service.h"
#include "convert.h"
#include "ephemeral_node_factory.h"
#include "tree_builder.h"
#include "ypath_client.h"
#include "ypath_detail.h"

#include <yt/core/profiling/timing.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/yson/async_consumer.h>
#include <yt/core/yson/attribute_consumer.h>
#include <yt/core/yson/writer.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/periodic_executor.h>

namespace NYT {
namespace NYTree {

using namespace NYson;
using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TFromProducerYPathService
    : public TYPathServiceBase
    , public TSupportsGet
    , public ICachedYPathService
{
public:
    TFromProducerYPathService(TYsonProducer producer, TDuration cachePeriod)
        : Producer_(std::move(producer))
        , CachePeriod_(cachePeriod)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        // Try to handle root get requests without constructing ephemeral YTree.
        if (path.empty() && context->GetMethod() == "Get") {
            return TResolveResultHere{path};
        } else {
            return TResolveResultThere{BuildNodeFromProducer(), path};
        }
    }

    virtual void SetCachePeriod(TDuration period) override
    {
        CachePeriod_ = period;
    }

private:
    const TYsonProducer Producer_;

    TYsonString CachedString_;
    INodePtr CachedNode_;
    TDuration CachePeriod_;
    TInstant LastStringUpdateTime_;
    TInstant LastNodeUpdateTime_;

    virtual bool DoInvoke(const IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return TYPathServiceBase::DoInvoke(context);
    }

    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        if (request->has_attributes())  {
            // Execute fallback.
            auto node = BuildNodeFromProducer();
            ExecuteVerb(node, IServiceContextPtr(context));
            return;
        }

        auto yson = BuildStringFromProducer();
        response->set_value(yson.GetData());
        context->Reply();
    }

    virtual void GetRecursive(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        Y_UNREACHABLE();
    }

    virtual void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        Y_UNREACHABLE();
    }


    TYsonString BuildStringFromProducer()
    {
        if (CachePeriod_ != TDuration()) {
            auto now = NProfiling::GetInstant();
            if (LastStringUpdateTime_ + CachePeriod_ > now) {
                return CachedString_;
            }
        }

        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        Producer_.Run(&writer);
        writer.Flush();

        const auto& str = stream.Str();
        if (str.empty()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "No data is available");
        }

        auto result = TYsonString(str);

        if (CachePeriod_ != TDuration()) {
            CachedString_ = result;
            LastStringUpdateTime_ = NProfiling::GetInstant();
        }

        return result;
    }

    INodePtr BuildNodeFromProducer()
    {
        if (CachePeriod_ != TDuration()) {
            auto now = NProfiling::GetInstant();
            if (LastNodeUpdateTime_ + CachePeriod_ > now) {
                return CachedNode_;
            }
        }

        auto result = ConvertTo<INodePtr>(BuildStringFromProducer());

        if (CachePeriod_ != TDuration()) {
            CachedNode_ = result;
            LastNodeUpdateTime_ = NProfiling::GetInstant();
        }

        return result;
    }
};

IYPathServicePtr IYPathService::FromProducer(TYsonProducer producer, TDuration cachePeriod)
{
    return New<TFromProducerYPathService>(producer, cachePeriod);
}

TYsonProducer IYPathService::ToProducer()
{
    return BIND([this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
        auto result = SyncYPathGet(this_, "");
        consumer->OnRaw(result);
    });
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
        const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    virtual bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr Invoker_;


    virtual bool DoInvoke(const IServiceContextPtr& context) override
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
    , public ICachedYPathService
{
public:
    TCachedYPathService(
        IYPathServicePtr underlyingService,
        TDuration updatePeriod,
        IInvokerPtr workerInvoker)
        : UnderlyingService_(std::move(underlyingService))
        , WorkerInvoker_(workerInvoker
            ? workerInvoker
            : NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            WorkerInvoker_,
            BIND(&TCachedYPathService::RebuildCache, MakeWeak(this)),
            updatePeriod))
    {
        YCHECK(UnderlyingService_);
        SetCachePeriod(updatePeriod);
    }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

    virtual void SetCachePeriod(TDuration period)
    {
        if (period == TDuration::Zero()) {
            if (IsCacheEnabled_) {
                IsCacheEnabled_ = false;
                PeriodicExecutor_->Stop();
            }
        } else {
            PeriodicExecutor_->SetPeriod(period);
            if (!IsCacheEnabled_) {
                IsCacheValid_ = false;
                IsCacheEnabled_ = true;
                PeriodicExecutor_->Start();
            }
        }
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr WorkerInvoker_;

    std::atomic<bool> IsCacheEnabled_ = {false};
    std::atomic<bool> IsCacheValid_ = {false};
    const TPeriodicExecutorPtr PeriodicExecutor_;

    TSpinLock SpinLock_;
    TErrorOr<INodePtr> CachedTreeOrError_;

    virtual bool DoInvoke(const IServiceContextPtr& context) override
    {
        if (IsCacheEnabled_ && IsCacheValid_) {
            WorkerInvoker_->Invoke(BIND([=, this_ = MakeStrong(this)]() {
                try {
                    auto cachedTree = GetCachedTree().ValueOrThrow();
                    ExecuteVerb(cachedTree, context);
                } catch (const std::exception& ex) {
                    context->Reply(ex);
                }
            }));
        } else {
            UnderlyingService_->Invoke(context);
        }
        return true;
    }

    void RebuildCache()
    {
        try {
            auto asyncYson = AsyncYPathGet(
                UnderlyingService_,
                TYPath(),
                Null);

            auto yson = WaitFor(asyncYson)
                .ValueOrThrow();

            SetCachedTree(ConvertToNode(yson));
        } catch (const std::exception& ex) {
            SetCachedTree(TError(ex));
        }
    }

    TErrorOr<INodePtr> GetCachedTree()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return CachedTreeOrError_;
    }

    void SetCachedTree(const TErrorOr<INodePtr>& cachedTreeOrError)
    {
        // NB: We must not be dropping the previous tree while holding
        // a spin lock since destroying the tree could take a while. Cf. YT-4948.
        decltype(CachedTreeOrError_) oldCachedTreeOrError;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            std::swap(CachedTreeOrError_, oldCachedTreeOrError);
            CachedTreeOrError_ = cachedTreeOrError;
            IsCacheValid_ = true;
        }
    }
};

IYPathServicePtr IYPathService::Cached(TDuration updatePeriod, IInvokerPtr workerInvoker)
{
    return New<TCachedYPathService>(this, updatePeriod, workerInvoker);
}

////////////////////////////////////////////////////////////////////////////////

class TPermissionValidatingYPathService
    : public TYPathServiceBase
    , public TSupportsPermissions
{
public:
    TPermissionValidatingYPathService(
        IYPathServicePtr underlyingService,
        TCallback<void(const TString&, EPermission)> validationCallback)
        : UnderlyingService_(std::move(underlyingService))
        , ValidationCallback_(std::move(validationCallback))
        , PermissionValidator_(this, EPermissionCheckScope::This)
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        return TResolveResultHere{path};
    }

    virtual bool ShouldHideAttributes() override
    {
        return UnderlyingService_->ShouldHideAttributes();
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const TCallback<void(const TString&, EPermission)> ValidationCallback_;

    TCachingPermissionValidator PermissionValidator_;

    virtual void ValidatePermission(
        EPermissionCheckScope /* scope */,
        EPermission permission,
        const TString& user) override
    {
        ValidationCallback_.Run(user, permission);
    }

    virtual bool DoInvoke(const IServiceContextPtr& context) override
    {
        // TODO(max42): choose permission depending on method.
        PermissionValidator_.Validate(EPermission::Read, context->GetUser());
        ExecuteVerb(UnderlyingService_, context);
        return true;
    }
};

IYPathServicePtr IYPathService::WithPermissionValidator(TCallback<void(const TString&, EPermission)> validationCallback)
{
    return New<TPermissionValidatingYPathService>(this, std::move(validationCallback));
}

////////////////////////////////////////////////////////////////////////////////

void IYPathService::WriteAttributesFragment(
    IAsyncYsonConsumer* consumer,
    const TNullable<std::vector<TString>>& attributeKeys,
    bool stable)
{
    if (!attributeKeys && ShouldHideAttributes() ||
        attributeKeys && attributeKeys->empty())
    {
        return;
    }
    DoWriteAttributesFragment(consumer, attributeKeys, stable);
}

void IYPathService::WriteAttributes(
    IAsyncYsonConsumer* consumer,
    const TNullable<std::vector<TString>>& attributeKeys,
    bool stable)
{
    TAttributeFragmentConsumer attributesConsumer(consumer);
    WriteAttributesFragment(&attributesConsumer, attributeKeys, stable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
