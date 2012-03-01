#include "stdafx.h"
#include "transaction.h"

#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/load_context.h>

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
    ::Save(output, Parent_->GetId());
    SaveSet(output, CreatedObjectIds_);
    ::Save(output, LockIds_);
    ::Save(output, BranchedNodeIds_);
    ::Save(output, CreatedNodeIds_);
}

void TTransaction::Load(TInputStream* input, const TLoadContext& context)
{
    TObjectWithIdBase::Load(input);
    ::Load(input, State_);
    LoadObjects(input, NestedTransactions_, context);
    TTransactionId parentId;
    ::Load(input, parentId);
    Parent_ = context.Get<TTransaction>(parentId);
    LoadSet(input, CreatedObjectIds_);
    ::Load(input, LockIds_);
    ::Load(input, BranchedNodeIds_);
    ::Load(input, CreatedNodeIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

