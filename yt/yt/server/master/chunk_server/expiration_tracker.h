#pragma once
#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NChunkServer {

///////////////////////////////////////////////////////////////////////////////

class TExpirationTracker
    : public TRefCounted
{
public:
    explicit TExpirationTracker(NCellMaster::TBootstrap* bootstrap);

    void Start();
    void Stop();

    void Clear();

    void ScheduleExpiration(TChunk* chunk);
    void CancelExpiration(TChunk* chunk);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    const TCallback<void(NCellMaster::TDynamicClusterConfigPtr)> DynamicConfigChangedCallback_ =
        BIND(&TExpirationTracker::OnDynamicConfigChanged, MakeWeak(this));

    NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    TChunkExpirationMap ExpirationMap_;
    THashSet<TChunk*> ExpiredChunks_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void RegisterChunkExpiration(TChunk* chunk, TInstant expirationTime);
    void UnregisterChunkExpiration(TChunk* chunk);

    void OnCheck();

    bool IsRecovery() const;

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(NCellMaster::TDynamicClusterConfigPtr oldConfig);
};

DEFINE_REFCOUNTED_TYPE(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
