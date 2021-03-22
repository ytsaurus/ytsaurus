#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/core/misc/ref_counted.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

class TTableManager
    : public TRefCounted
{
public:
    explicit TTableManager(NCellMaster::TBootstrap* bootstrap);
    virtual ~TTableManager() override;

    void Initialize();

    void ScheduleStatisticsUpdate(
        NChunkServer::TChunkOwnerBase* chunkOwner,
        bool updateDataStatistics = true,
        bool updateTabletStatistics = true,
        bool useNativeContentRevisionCas = false);

    void SendStatisticsUpdate(
        NChunkServer::TChunkOwnerBase* chunkOwner,
        bool useNativeContentRevisionCas = false);

    // COMPAT(shakurov)
    void LoadStatisticsUpdateRequests(NCellMaster::TLoadContext& context);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTableManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
