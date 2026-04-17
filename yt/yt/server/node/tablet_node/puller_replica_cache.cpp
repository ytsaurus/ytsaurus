#include "puller_replica_cache.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NTabletNode {

using namespace NChaosClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TDummyPullerReplicaCache
    : public IPullerReplicaCache
{
public:
    void OnPull(TTabletId /*tabletId*/) override
    { }
};

DEFINE_REFCOUNTED_TYPE(TDummyPullerReplicaCache)

////////////////////////////////////////////////////////////////////////////////

class TPullerReplicaCache
    : public IPullerReplicaCache
    , public TAsyncExpiringCache<TTabletId, void>
{
public:
    TPullerReplicaCache(TTablet* tablet, TReplicationCardId replicationCardId)
        : TAsyncExpiringCache<TTabletId, void>(
            GetCacheConfig(tablet->GetSettings().MountConfig),
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TabletNodeLogger().WithTag("%v, ReplicationCardId: %v",
                tablet->GetLoggingTag(),
                replicationCardId))
        , ExportSizeThreshold_(tablet->GetSettings().MountConfig->PullerReplicaCacheExportSizeThreshold)
        , SizeGauge_(tablet->GetTableProfiler()->GetProfiler()
            .WithPrefix("/tablet/puller_replica_cache")
            .WithSparse()
            .GaugeSummary("/size", ESummaryPolicy::Max))
    { }

    void OnPull(TTabletId pullerTabletId) override
    {
        Y_UNUSED(Get(pullerTabletId));
    }

    void OnAdded(const TTabletId& /*key*/) noexcept override
    {
        ++Size_;

        if (Size_ >= ExportSizeThreshold_) {
            ExportSize_ = true;
        }

        if (ExportSize_) {
            SizeGauge_.Update(Size_);
        }
    }

    void OnRemoved(const TTabletId& /*key*/) noexcept override
    {
        --Size_;

        if (ExportSize_) {
            SizeGauge_.Update(Size_);
        }
    }

private:
    const int ExportSizeThreshold_;
    const NProfiling::TGauge SizeGauge_;

    int Size_ = 0;
    bool ExportSize_ = false;

    TFuture<void> DoGet(
        const TTabletId& /*key*/,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        return OKFuture;
    }

    static TAsyncExpiringCacheConfigPtr GetCacheConfig(const TTableMountConfigPtr& mountConfig)
    {
        auto config = New<TAsyncExpiringCacheConfig>();
        if (mountConfig->PullerReplicaCacheTimeout) {
            config->ExpireAfterAccessTime = *mountConfig->PullerReplicaCacheTimeout;
        }
        return config;
    }
};

DEFINE_REFCOUNTED_TYPE(TPullerReplicaCache)

////////////////////////////////////////////////////////////////////////////////

IPullerReplicaCachePtr GetDisabledPullerReplicaCache()
{
    return LeakyRefCountedSingleton<TDummyPullerReplicaCache>();
}

IPullerReplicaCachePtr CreatePullerReplicaCache(
    TTablet* tablet,
    TReplicationCardId replicationCardId)
{
    return New<TPullerReplicaCache>(tablet, replicationCardId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
