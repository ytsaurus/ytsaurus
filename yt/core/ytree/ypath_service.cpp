#include "ypath_service.h"
#include "convert.h"
#include "ephemeral_node_factory.h"
#include "tree_builder.h"
#include "ypath_client.h"
#include "ypath_detail.h"

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
{
public:
    explicit TFromProducerYPathService(TYsonProducer producer)
        : Producer_(std::move(producer))
    { }

    virtual TResolveResult Resolve(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        // Try to handle root get requests without constructing ephemeral YTree.
        if (path.empty() && context->GetMethod() == "Get") {
            return TResolveResultHere{path};
        } else {
            auto node = BuildNodeFromProducer();
            return TResolveResultThere{std::move(node), path};
        }
    }

private:
    const TYsonProducer Producer_;


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
        TStringStream stream;
        TBufferedBinaryYsonWriter writer(&stream);
        Producer_.Run(&writer);
        writer.Flush();

        const auto& str = stream.Str();
        if (str.empty()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "No data is available");
        }

        return TYsonString(str);
    }

    INodePtr BuildNodeFromProducer()
    {
        return ConvertTo<INodePtr>(BuildStringFromProducer());
    }
};

IYPathServicePtr IYPathService::FromProducer(TYsonProducer producer)
{
    return New<TFromProducerYPathService>(producer);
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
{
public:
    TCachedYPathService(
        IYPathServicePtr underlyingService,
        TDuration updatePeriod)
        : UnderlyingService_(std::move(underlyingService))
        , PeriodicExecutor_(New<TPeriodicExecutor>(
            GetWorkerInvoker(),
            BIND(&TCachedYPathService::RebuildCache, MakeWeak(this)),
            updatePeriod))
    {
        YCHECK(UnderlyingService_);
        // NB: we explicitly build cache in constructor in order to have
        // CachedTreeOrError_ set at moment of first request invocation.
        RebuildCache();
        PeriodicExecutor_->Start();
    }

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultHere{path};
    }

private:
    const IYPathServicePtr UnderlyingService_;
    const TPeriodicExecutorPtr PeriodicExecutor_;

    TSpinLock SpinLock_;
    TErrorOr<INodePtr> CachedTreeOrError_;

    virtual bool DoInvoke(const IServiceContextPtr& context) override
    {
        GetWorkerInvoker()->Invoke(BIND([=, this_ = MakeStrong(this)] () {
            try {
                auto cachedTreeOrError = GetCachedTree();
                auto cachedTree = cachedTreeOrError.ValueOrThrow();
                ExecuteVerb(cachedTree, context);
            } catch (const std::exception& ex) {
                context->Reply(ex);
            }
        }));
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

            auto node = ConvertToNode(yson);

            SetCachedTree(node);
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
        }
    }

    static IInvokerPtr GetWorkerInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }
};

IYPathServicePtr IYPathService::Cached(TDuration updatePeriod)
{
    return updatePeriod == TDuration::Zero()
        ? MakeStrong(this)
        : New<TCachedYPathService>(this, updatePeriod);
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
        : UnderlyingService_(underlyingService)
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

IYPathServicePtr IYPathService::AddPermissionValidator(TCallback<void(const TString&, EPermission)> validationCallback)
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
