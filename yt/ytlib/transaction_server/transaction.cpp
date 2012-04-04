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
    SaveObjects(output, NestedTransactions_);
    SaveObject(output, Parent_);
    SaveSet(output, CreatedObjectIds_);
    SaveObjects(output, Locks_);
    ::Save(output, BranchedNodeIds_);
    ::Save(output, CreatedNodeIds_);
}

void TTransaction::Load(const TLoadContext& context, TInputStream* input)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, State_);
    LoadObjects(input, NestedTransactions_, context);
    LoadObject(input, Parent_, context);
    LoadSet(input, CreatedObjectIds_);
    LoadObjects(input, Locks_, context);
    ::Load(input, BranchedNodeIds_);
    ::Load(input, CreatedNodeIds_);
}

TTransactionId NYT::NTransactionServer::TTransaction::GetParentId() const
{
    return Parent_ ? Parent_->GetId() : NullTransactionId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

