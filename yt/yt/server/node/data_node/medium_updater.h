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
        TMediumDirectoryManagerPtr mediumDirectoryManager);

    void UpdateLocationMedia(
        const NDataNodeTrackerClient::NProto::TMediumOverrides& mediumOverrides,
        bool onInitialize = false);

private:
    IBootstrap* const Bootstrap_;
    const TMediumDirectoryManagerPtr MediumDirectoryManager_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    NChunkClient::TMediumDirectoryPtr GetMediumDirectoryOrCrash(bool onInitialize);
};

DEFINE_REFCOUNTED_TYPE(TMediumUpdater)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
