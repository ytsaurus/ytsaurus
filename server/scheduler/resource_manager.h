#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceManager
    : public TRefCounted
{
public:
    explicit TResourceManager(NServer::NMaster::TBootstrap* bootstrap);

    void AssignPodToNode(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TNode* node,
        NObjects::TPod* pod);

    void RevokePodFromNode(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    void PrepareUpdatePodSpec(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);
    void UpdatePodSpec(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

    void ValidateNodeResource(NObjects::TNode* node);

    void ReallocatePodResources(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
