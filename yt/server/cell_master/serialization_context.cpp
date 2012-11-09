#include "stdafx.h"
#include "serialization_context.h"
#include "bootstrap.h"

#include <server/transaction_server/transaction_manager.h>

#include <server/object_server/object_detail.h>

#include <server/chunk_server/chunk_list.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/cypress_server/cypress_manager.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

NMetaState::TVersionValidator SnapshotVersionValidator()
{
    static auto result = BIND([] (int version) {
        YCHECK(version == 1 ||
               version == 2 ||
               version == 3);
    });
    return result;
}

template <>
TTransaction* TLoadContext::Get(const TObjectId& id) const
{
    return Bootstrap_->GetTransactionManager()->GetTransaction(id);
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
ICypressNode* TLoadContext::Get(const TVersionedObjectId& id) const
{
    return Bootstrap_->GetCypressManager()->GetNode(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
