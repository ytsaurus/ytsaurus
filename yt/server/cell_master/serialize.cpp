#include "stdafx.h"
#include "serialize.h"
#include "bootstrap.h"

#include <ytlib/object_client/helpers.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/transaction_server/transaction_manager.h>

#include <server/object_server/object_detail.h>

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_owner_base.h>
#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/job.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/user.h>
#include <server/security_server/group.h>

#include <server/table_server/table_node.h>

#include <server/tablet_server/tablet_manager.h>
#include <server/tablet_server/tablet_cell.h>

namespace NYT {
namespace NCellMaster {

using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NTabletServer;
using namespace NTableServer;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 105;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 43 ||
        version == 100 ||
        version == 101 ||
        version == 102 ||
        version == 103 ||
        version == 104 ||
        version == 105;
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

template <>
TObjectBase* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetObjectManager()->GetObject(id);
}

template <>
TTransaction* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetTransactionManager()->GetTransaction(id);
}

template <>
NCypressServer::TLock* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetCypressManager()->GetLock(id);
}

template <>
TChunkTree* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetChunkManager()->GetChunkTree(id);
}

template <>
TChunkList* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetChunkManager()->GetChunkList(id);
}

template <>
TChunk* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetChunkManager()->GetChunk(id);
}

template <>
TChunkOwnerBase* TLoadContext::Get(const TVersionedNodeId& id) const
{
    return static_cast<TChunkOwnerBase*>(Get<TCypressNodeBase>(id));
}

template <>
TCypressNodeBase* TLoadContext::Get(const NCypressClient::TNodeId& id) const
{
    return Bootstrap_->GetCypressManager()->GetNode(TVersionedNodeId(id));
}

template <>
TCypressNodeBase* TLoadContext::Get(const TVersionedNodeId& id) const
{
    return Bootstrap_->GetCypressManager()->GetNode(id);
}

template <>
TAccount* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetSecurityManager()->GetAccount(id);
}

template <>
TNode* TLoadContext::Get(NNodeTrackerServer::TNodeId id) const
{
    return Bootstrap_->GetNodeTracker()->GetNode(id);
}

template <>
TSubject* TLoadContext::Get(const TObjectId& id) const
{
    switch (TypeFromId(id)) {
        case EObjectType::User:  return Get<TUser>(id);
        case EObjectType::Group: return Get<TGroup>(id);
        default:                 YUNREACHABLE();
    }
}

template <>
TUser* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetSecurityManager()->GetUser(id);
}

template <>
TGroup* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetSecurityManager()->GetGroup(id);
}

template <>
TTableNode* TLoadContext::Get(const TVersionedNodeId& id) const
{
    auto* node = Bootstrap_->GetCypressManager()->GetNode(id);
    YCHECK(node->GetType() == EObjectType::Table);
    return static_cast<TTableNode*>(node);
}

template <>
TTabletCell* TLoadContext::Get(const TTabletCellId& id) const
{
    return Bootstrap_->GetTabletManager()->GetTabletCell(id);
}

template <>
TTablet* TLoadContext::Get(const TTabletId& id) const
{
    return Bootstrap_->GetTabletManager()->GetTablet(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
