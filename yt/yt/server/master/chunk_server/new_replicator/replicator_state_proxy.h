#include "public.h"

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NChunkServer::NReplicator {

////////////////////////////////////////////////////////////////////////////////

struct IReplicatorStateProxy
{
    virtual ~IReplicatorStateProxy() = default;

    virtual const IInvokerPtr& GetChunkInvoker(EChunkThreadQueue queue) const = 0;

    virtual const NCellMaster::TDynamicClusterConfigPtr& GetDynamicConfig() const = 0;

    virtual std::vector<NChunkServer::TMedium*> GetMedia() const = 0;
    virtual std::vector<NNodeTrackerServer::TDataCenter*> GetDataCenters() const = 0;

    virtual bool CheckThreadAffinity() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IReplicatorStateProxy> CreateReplicatorStateProxy(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer::NReplicator
