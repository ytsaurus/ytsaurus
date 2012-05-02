#include "stdafx.h"
#include "transaction.h"

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/load_context.h>
#include <ytlib/cypress/lock.h>

#include <util/ysaveload.h>

namespace NYT {
namespace NTransactionServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TObjectWithIdBase(id)
    , Parent_(NULL)
{ }

void TTransaction::Save(TOutputStream* output) const
{
    TObjectWithIdBase::Save(output);
    ::Save(output, State_);
    SaveObjectRefs(output, NestedTransactions_);
    SaveObjectRef(output, Parent_);
    SaveSet(output, CreatedObjectIds_);
    SaveObjectRefs(output, Locks_);
    SaveObjectRefs(output, BranchedNodes_);
    SaveObjectRefs(output, CreatedNodes_);
}

void TTransaction::Load(const TLoadContext& context, TInputStream* input)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, State_);
    LoadObjectRefs(input, NestedTransactions_, context);
    LoadObjectRef(input, Parent_, context);
    LoadSet(input, CreatedObjectIds_);
    LoadObjectRefs(input, Locks_, context);
    LoadObjectRefs(input, BranchedNodes_, context);
    LoadObjectRefs(input, CreatedNodes_, context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

