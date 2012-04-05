#include "stdafx.h"
#include "load_context.h"

#include "bootstrap.h"

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/object_server/id.h>
#include <ytlib/chunk_server/chunk_list.h>
#include <ytlib/cypress/lock.h>
#include <ytlib/cypress/cypress_manager.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap *bootstrap)
    : Bootstrap_(bootstrap)
{ }

template <>
TTransaction* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetTransactionManager()->GetTransaction(id);
}

template <>
TChunkList* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetChunkManager()->GetChunkList(id);
}

template <>
TChunk* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetChunkManager()->GetChunk(id);
}

template <>
TJob* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetChunkManager()->GetJob(id);
}

template <>
TLock* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetCypressManager()->GetLock(id);
}
////////////////////////////////////////////////////////////////////////////////

void SaveObject(TOutputStream* output, const TObjectWithIdBase* object)
{
    ::Save(output, object ? object->GetId() : NullObjectId);
}

////////////////////////////////////////////////////////////////////////////////

template <>
void SaveObjects(TOutputStream* output, const std::vector<TChunkTreeRef>& objects)
{
    ::SaveSize(output, objects.size());
    FOREACH (const auto& object, objects) {
        ::Save(output, object.GetId());
    }
}

void LoadObjects(TInputStream* input, std::vector<TChunkTreeRef>& objects, const TLoadContext& context)
{
    auto size = ::LoadSize(input);
    objects.reserve(size);
    for (size_t i = 0; i < size; ++i) {
        TObjectId id;
        ::Load(input, id);

        if (TypeFromId(id) == EObjectType::ChunkList) {
            objects.push_back(TChunkTreeRef(context.Get<TChunkList>(id)));
        } else {
            objects.push_back(TChunkTreeRef(context.Get<TChunk>(id)));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
