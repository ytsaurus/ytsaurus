#pragma once

#include "object.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TCluster
    : public NYT::TRefCounted
{
public:
    explicit TCluster(NMaster::TBootstrap* bootstrap);

    std::vector<TNode*> GetNodes();
    TNode* FindNode(const TObjectId& id);
    TNode* GetNodeOrThrow(const TObjectId& id);
    int GetNodeCount();

    std::vector<TPod*> GetPods();
    TPod* FindPod(const TObjectId& id);
    TPod* GetPodOrThrow(const TObjectId& id);
    int GetPodCount();

    void LoadSnapshot();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCluster)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
