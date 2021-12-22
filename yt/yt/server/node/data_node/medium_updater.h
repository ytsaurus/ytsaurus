#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/data_node_tracker_client/proto/data_node_tracker_service.pb.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TMediumUpdater
    : public TRefCounted
{
public:
    TMediumUpdater(
        IBootstrap* bootstrap,
        TMediumDirectoryManagerPtr mediumDirectoryManager,
        TDuration legacyMediumUpdaterPeriod);
    
    void UpdateLocationMedia(
        const NDataNodeTrackerClient::NProto::TMediumOverrides& mediumOverrides,
        bool onInitialize = false);
    
    void LegacyInitializeLocationMedia();
    void EnableLegacyMode(bool legacyModeIsEnabled);

    void SetPeriod(TDuration legacyMediumUpdatePeriod);

private:
    using TLegacyMediumOverrides = THashMap<NChunkClient::TLocationUuid, TString>;
    using TMediumOverrides = THashMap<NChunkClient::TLocationUuid, int>;

    void OnLegacyUpdateLocationMedia(bool onInitialize = false);
    void DoLegacyUpdateMedia(const TLegacyMediumOverrides& mediumOverrides, bool onInitialize = false);
    void DoUpdateMedia(const TMediumOverrides& mediumOverrides, bool onInitialize = false);

    IBootstrap* const Bootstrap_;

    // COMPAT(kvk1920): Remove periodic MediumUpdater.
    const IInvokerPtr ControlInvoker_;
    NConcurrency::TPeriodicExecutorPtr LegacyUpdateLocationMediaExecutor_;
    bool UseHeartbeats_;

    TMediumDirectoryManagerPtr MediumDirectoryManager_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

DEFINE_REFCOUNTED_TYPE(TMediumUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
