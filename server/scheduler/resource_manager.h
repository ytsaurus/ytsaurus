#pragma once

#include "public.h"

#include <yp/server/net/public.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TResourceManagerContext
{
    NNet::TNetManager* NetManager = nullptr;
    NNet::TInternetAddressManager* InternetAddressManager = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceManager
    : public TRefCounted
{
public:
    explicit TResourceManager(NServer::NMaster::TBootstrap* bootstrap);

    void AssignPodToNode(
        const NObjects::TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TNode* node,
        NObjects::TPod* pod);

    void RevokePodFromNode(
        const NObjects::TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TPod* pod);

    void RemoveOrphanedAllocations(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TNode* node);

    void PrepareUpdatePodSpec(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    void UpdatePodSpec(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    void ValidateNodeResource(NObjects::TNode* node);

    void ReallocatePodResources(
        const NObjects::TTransactionPtr& transaction,
        TResourceManagerContext* context,
        NObjects::TPod* pod);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
