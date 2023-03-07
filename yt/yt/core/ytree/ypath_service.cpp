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

#include <yt/core/misc/atomic_object.h>
#include <yt/core/misc/checksum.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TCacheKey
{
    TYPath Path;
    TString Method;
    TSharedRef RequestBody;
    TChecksum RequestBodyHash;

    TCacheKey(
        const TYPath& path,
        const TString& method,
        const TSharedRef& requestBody)
        : Path(path)
        , Method(method)
        , RequestBody(requestBody)
        , RequestBodyHash(GetChecksum(RequestBody))
    { }

    bool operator == (const TCacheKey& other) const
    {
        return
            Path == other.Path &&
            Method == other.Method &&
            RequestBodyHash == other.RequestBodyHash &&
            TRef::AreBitwiseEqual(RequestBody, other.RequestBody);
    }

    friend TString ToString(const TCacheKey& key)
    {
        return Format("{%v %v %x}",
            key.Method,
            key.Path,
            key.RequestBodyHash);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

template <>
struct THash<NYT::NYTree::TCacheKey>
{
    size_t operator()(const NYT::NYTree::TCacheKey& key) const {
        size_t result = 0;
        NYT::HashCombine(result, key.Path);
        NYT::HashCombine(result, key.Method);
        NYT::HashCombine(result, key.RequestBodyHash);
        return result;
    }
};

namespace NYT::NYTree {

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
        YT_ABORT();
    }

    virtual void GetAttribute(const TYPath& /*path*/, TReqGet* /*request*/, TRspGet* /*response*/, const TCtxGetPtr& /*context*/) override
    {
        YT_ABORT();
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

DECLARE_REFCOUNTED_STRUCT(TCacheProfilingCounters);

struct TCacheProfilingCounters
    : public TRefCounted
{
    explicit TCacheProfilingCounters(const NProfiling::TProfiler& profiler)
        : Profiler(profiler)
        , CacheHitCounter("/cache_hit")
        , CacheMissCounter("/cache_miss")
        , RedundantCacheMissCounter("/redundant_cache_miss")
        , ByteSize("/byte_size")
    { }

    const NProfiling::TProfiler Profiler;
    NProfiling::TMonotonicCounter CacheHitCounter;
    NProfiling::TMonotonicCounter CacheMissCounter;
    NProfiling::TMonotonicCounter RedundantCacheMissCounter;
    NProfiling::TMonotonicCounter InvalidCacheCounter;
    NProfiling::TSimpleGauge ByteSize;
};

DEFINE_REFCOUNTED_TYPE(TCacheProfilingCounters);

DECLARE_REFCOUNTED_CLASS(TCacheSnapshot);

class TCacheSnapshot
    : public TRefCounted
{
public:
    TCacheSnapshot(const TErrorOr<INodePtr>& treeOrError, const TCacheProfilingCountersPtr& profilingCounters)
        : TreeOrError_(treeOrError)
        , ProfilingCounters_(profilingCounters)
    { }

    const TErrorOr<INodePtr>& GetTreeOrError() const
    {
        return TreeOrError_;
    }

    void AddResponseToCache(const TCacheKey& key, const TErrorOr<TSharedRefArray>& response)
    {
        auto guard = TWriterGuard(Lock_);

        decltype(CachedReplies_)::insert_ctx ctx;
        auto it = CachedReplies_.find(key, ctx);

        if (it == CachedReplies_.end()) {
            CachedReplies_.emplace_direct(ctx, key, response);
        } else {
            ProfilingCounters_->Profiler.Increment(ProfilingCounters_->RedundantCacheMissCounter);
        }
    }

    std::optional<TErrorOr<TSharedRefArray>> LookupResponse(const TCacheKey& key) const
    {
        auto guard = TReaderGuard(Lock_);

        auto it = CachedReplies_.find(key);
        if (it == CachedReplies_.end()) {
            return std::nullopt;
        } else {
            return it->second;
        }
    }

private:
    const TErrorOr<INodePtr> TreeOrError_;
    TReaderWriterSpinLock Lock_;
    THashMap<TCacheKey, TErrorOr<TSharedRefArray>> CachedReplies_;
    TCacheProfilingCountersPtr ProfilingCounters_;
};

DEFINE_REFCOUNTED_TYPE(TCacheSnapshot);

DECLARE_REFCOUNTED_CLASS(TCachedYPathServiceContext)

class TCachedYPathServiceContext
    : public TServiceContextWrapper
{
public:
    TCachedYPathServiceContext(
        IServiceContextPtr underlyingContext,
        TWeakPtr<TCacheSnapshot> cacheSnapshot,
        TCacheKey cacheKey)
        : TServiceContextWrapper(std::move(underlyingContext))
        , CacheSnapshot_(std::move(cacheSnapshot))
        , CacheKey_(std::move(cacheKey))
    { }

    virtual void Reply(const TError& error) override
    {
        TryAddResponseToCache(error);
        TServiceContextWrapper::Reply(error);
    }

    virtual void Reply(const TSharedRefArray& responseMessage) override
    {
        TryAddResponseToCache(responseMessage);
        TServiceContextWrapper::Reply(responseMessage);
    }

private:
    typedef TIntrusivePtr<TCacheSnapshot> TCacheSnapshotPtr;

    const TWeakPtr<TCacheSnapshot> CacheSnapshot_;
    const TCacheKey CacheKey_;

    void TryAddResponseToCache(const TErrorOr<TSharedRefArray>& response)
    {
        auto cacheSnapshot = CacheSnapshot_.Lock();
        if (cacheSnapshot) {
            cacheSnapshot->AddResponseToCache(CacheKey_, response);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TCachedYPathServiceContext);

DECLARE_REFCOUNTED_CLASS(TCachedYPathService);

class TCachedYPathService
    : public TYPathServiceBase
    , public ICachedYPathService
{
public:
    TCachedYPathService(
        IYPathServicePtr underlyingService,
        TDuration updatePeriod,
        IInvokerPtr workerInvoker,
        const NProfiling::TProfiler& profiler);

    virtual TResolveResult Resolve(const TYPath& path, const IServiceContextPtr& /*context*/) override;

    virtual void SetCachePeriod(TDuration period) override;

private:
    const IYPathServicePtr UnderlyingService_;
    const IInvokerPtr WorkerInvoker_;

    std::atomic<bool> IsCacheEnabled_ = {false};
    std::atomic<bool> IsCacheValid_ = {false};
    const TPeriodicExecutorPtr PeriodicExecutor_;

    TCacheProfilingCountersPtr ProfilingCounters_;

    TAtomicObject<TCacheSnapshotPtr> CurrentCacheSnapshot_ = nullptr;

    virtual bool DoInvoke(const IServiceContextPtr& context) override;

    void RebuildCache();

    void UpdateCachedTree(const TErrorOr<INodePtr>& treeOrError);
};

DEFINE_REFCOUNTED_TYPE(TCachedYPathService);

TCachedYPathService::TCachedYPathService(
    IYPathServicePtr underlyingService,
    TDuration updatePeriod,
    IInvokerPtr workerInvoker,
    const NProfiling::TProfiler& profiler)
    : UnderlyingService_(std::move(underlyingService))
    , WorkerInvoker_(workerInvoker
        ? workerInvoker
        : NRpc::TDispatcher::Get()->GetHeavyInvoker())
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        WorkerInvoker_,
        BIND(&TCachedYPathService::RebuildCache, MakeWeak(this)),
        updatePeriod))
    , ProfilingCounters_(New<TCacheProfilingCounters>(profiler))
{
    YT_VERIFY(UnderlyingService_);
    SetCachePeriod(updatePeriod);
}

IYPathService::TResolveResult TCachedYPathService::Resolve(const TYPath& path, const IServiceContextPtr&)
{
    return TResolveResultHere{path};
}

void TCachedYPathService::SetCachePeriod(TDuration period)
{
    if (period == TDuration::Zero()) {
        if (IsCacheEnabled_) {
            IsCacheEnabled_.store(false);
            PeriodicExecutor_->Stop();
        }
    } else {
        PeriodicExecutor_->SetPeriod(period);
        if (!IsCacheEnabled_) {
            IsCacheEnabled_.store(true);
            IsCacheValid_.store(false);
            PeriodicExecutor_->Start();
        }
    }
}

void ReplyErrorOrValue(const IServiceContextPtr& context, const TErrorOr<TSharedRefArray>& response)
{
    if (response.IsOK()) {
        context->Reply(response.Value());
    } else {
        context->Reply(static_cast<TError>(response));
    }
}

bool TCachedYPathService::DoInvoke(const IServiceContextPtr& context)
{
    if (IsCacheEnabled_ && IsCacheValid_) {
        WorkerInvoker_->Invoke(BIND([this, context, this_ = MakeStrong(this)]() {
            try {
                auto cacheSnapshot = CurrentCacheSnapshot_.Load();
                YT_VERIFY(cacheSnapshot);

                if (context->GetRequestMessage().Size() < 2) {
                    context->Reply(TError("Invalid request"));
                    return;
                }

                TCacheKey key(
                    GetRequestTargetYPath(context->GetRequestHeader()),
                    context->GetRequestHeader().method(),
                    context->GetRequestMessage()[1]);

                auto cachedResponse = cacheSnapshot->LookupResponse(key);
                if (cachedResponse) {
                    ReplyErrorOrValue(context, *cachedResponse);
                    ProfilingCounters_->Profiler.Increment(ProfilingCounters_->CacheHitCounter);
                    return;
                }

                auto treeOrError = cacheSnapshot->GetTreeOrError();
                if (!treeOrError.IsOK()) {
                    context->Reply(static_cast<TError>(treeOrError));
                    return;
                }
                auto tree = treeOrError.Value();

                auto contextWrapper = New<TCachedYPathServiceContext>(context, MakeWeak(cacheSnapshot), std::move(key));
                ExecuteVerb(tree, contextWrapper);
                ProfilingCounters_->Profiler.Increment(ProfilingCounters_->CacheMissCounter);
            } catch (const std::exception& ex) {
                context->Reply(ex);
            }
        }));
    } else {
        UnderlyingService_->Invoke(context);
        ProfilingCounters_->Profiler.Increment(ProfilingCounters_->InvalidCacheCounter);
    }

    return true;
}

void TCachedYPathService::RebuildCache()
{
    try {
        auto asyncYson = AsyncYPathGet(UnderlyingService_, /* path */ TYPath(), /* attributeKeys */ std::nullopt);

        auto yson = WaitFor(asyncYson)
            .ValueOrThrow();

        ProfilingCounters_->Profiler.Update(ProfilingCounters_->ByteSize, yson.GetData().Size());

        UpdateCachedTree(ConvertToNode(yson));
    } catch (const std::exception& ex) {
        UpdateCachedTree(TError(ex));
    }
}

void TCachedYPathService::UpdateCachedTree(const TErrorOr<INodePtr>& treeOrError)
{
    auto newCachedTree = New<TCacheSnapshot>(treeOrError, ProfilingCounters_);
    CurrentCacheSnapshot_.Store(newCachedTree);
    IsCacheValid_ = true;
}

IYPathServicePtr IYPathService::Cached(
    TDuration updatePeriod,
    IInvokerPtr workerInvoker,
    const NProfiling::TProfiler& profiler)
{
    return New<TCachedYPathService>(this, updatePeriod, workerInvoker, profiler);
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
    const std::optional<std::vector<TString>>& attributeKeys,
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
    const std::optional<std::vector<TString>>& attributeKeys,
    bool stable)
{
    TAttributeFragmentConsumer attributesConsumer(consumer);
    WriteAttributesFragment(&attributesConsumer, attributeKeys, stable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
