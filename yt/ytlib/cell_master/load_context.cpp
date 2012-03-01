#include "stdafx.h"
#include "load_context.h"

#include "bootstrap.h"

#include <ytlib/transaction_server/transaction_manager.h>
#include <ytlib/object_server/object_detail.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(TBootstrap *bootstrap)
    : Bootstrap_(bootstrap)
{ }

template <>
TTransaction* TLoadContext::Get(const TObjectId& id) const
{
    return &Bootstrap_->GetTransactionManager()->GetTransaction(id);
}

////////////////////////////////////////////////////////////////////////////////

void SaveObject(TOutputStream* output, const TObjectWithIdBase* object)
{
    ::Save(output, object ? object->GetId() : NullObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
