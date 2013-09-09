#pragma once

#include "public.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_owner_ypath_proxy.h>

#include <server/cypress_server/public.h>
#include <server/cypress_server/node.h>

#include <core/misc/property.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

// Base classes for cypress nodes that own chunks.
class TChunkOwnerBase
    : public NCypressServer::TCypressNodeBase
{
    DEFINE_BYVAL_RW_PROPERTY(NChunkServer::TChunkList*, ChunkList);
    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::EUpdateMode, UpdateMode);
    DEFINE_BYVAL_RW_PROPERTY(int, ReplicationFactor);
    DEFINE_BYVAL_RW_PROPERTY(bool, Vital);

public:
    explicit TChunkOwnerBase(const NCypressServer::TVersionedNodeId& id);

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

private:
    const NChunkServer::TChunkList* GetUsageChunkList() const;

};

bool CompareObjectsForSerialization(const TChunkOwnerBase* lhs, const TChunkOwnerBase* rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
