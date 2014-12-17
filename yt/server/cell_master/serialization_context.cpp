#include "stdafx.h"
#include "serialization_context.h"
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

namespace NYT {
namespace NCellMaster {

using namespace NNodeTrackerServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 46;
}

NMetaState::TVersionValidator SnapshotVersionValidator()
{
    static auto result = BIND([] (int version) {
        YCHECK(version == 10 ||
               version == 20 ||
               version == 21 ||
               version == 22 ||
               version == 23 ||
               version == 24 ||
               version == 25 ||
               version == 26 ||
               version == 27 ||
               version == 40 ||
               version == 41 ||
               version == 42 ||
               version == 43 ||
               version == 44 ||
               version == 45 ||
               version == 46);
    });
    return result;
}

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
