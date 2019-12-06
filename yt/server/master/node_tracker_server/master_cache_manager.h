#include "node_tracker.h"

#include <yt/server/master/cell_master/hydra_facade.h>

#include <yt/core/concurrency/public.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheManager
    : public TRefCounted
{
public:
    explicit TMasterCacheManager(NCellMaster::TBootstrap* bootstrap);
    void Reconfigure(TMasterCacheManagerConfigPtr config);

private:
    NCellMaster::TBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    TMasterCacheManagerConfigPtr Config_;

    void OnDynamicConfigChanged();
    void OnLeaderActive();
    void OnStopLeading();
    bool IsGoodNode(const TNode* node) const;
    THashMap<TRack*, int> CountNodesPerRack(const std::vector<TNode*>& nodes);
    std::vector<TNode*> FindAppropriateNodes(const std::vector<TNode*>& selectedNodes, int count);
    void UpdateMasterCacheNodes();
    void CommitMasterCacheNodes(const std::vector<TNode*>& nodeIds);
};

DEFINE_REFCOUNTED_TYPE(TMasterCacheManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
