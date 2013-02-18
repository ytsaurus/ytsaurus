#include "stdafx.h"
#include "serialization_context.h"
#include "bootstrap.h"

#include <server/transaction_server/transaction_manager.h>

#include <server/object_server/object_detail.h>

#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/security_server/security_manager.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NSecurityServer;

////////////////////////////////////////////////////////////////////////////////

NMetaState::TVersionValidator SnapshotVersionValidator()
{
    static auto result = BIND([] (int version) {
        YCHECK(version == 7 ||
               version == 8);
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
TJob* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetChunkManager()->GetJob(id);
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
TDataNode* TLoadContext::Get(NChunkServer::TNodeId id) const
{
    return Bootstrap_->GetChunkManager()->GetNode(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
