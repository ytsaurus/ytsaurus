#include "stdafx.h"
#include "load_context.h"

#include "bootstrap.h"

#include <ytlib/transaction_server/transaction_manager.h>

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
            
} // namespace NCellMaster
} // namespace NYT
