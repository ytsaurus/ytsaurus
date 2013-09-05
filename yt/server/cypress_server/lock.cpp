#include "stdafx.h"
#include "lock.h"

#include <ytlib/misc/serialize.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TLockRequest::TLockRequest()
{ }

TLockRequest::TLockRequest(ELockMode mode)
    : Mode(mode)
{ }

TLockRequest::TLockRequest(ELockMode::EDomain mode)
    : Mode(mode)
{ }

TLockRequest TLockRequest::SharedChild(const Stroka& key)
{
    TLockRequest result(ELockMode::Shared);
    result.ChildKey = key;
    return result;
}

TLockRequest TLockRequest::SharedAttribute(const Stroka& key)
{
    TLockRequest result(ELockMode::Shared);
    result.AttributeKey = key;
    return result;
}

void TLockRequest::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Mode);
    Save(context, ChildKey);
    Save(context, AttributeKey);
}

void TLockRequest::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Mode);
    Load(context, ChildKey);
    Load(context, AttributeKey);
}

////////////////////////////////////////////////////////////////////////////////

void Save(NCellMaster::TSaveContext& context, const TTransactionLockState& lockState)
{
    Save(context, lockState.Mode);
    Save(context, lockState.ChildKeys);
    Save(context, lockState.AttributeKeys);
}

void Load(NCellMaster::TLoadContext& context, TTransactionLockState& lockState)
{
    Load(context, lockState.Mode);
    Load(context, lockState.ChildKeys);
    Load(context, lockState.AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(const TLockId& id)
    : TNonversionedObjectBase(id)
    , State_(ELockState::Pending)
    , TrunkNode_(nullptr)
    , Transaction_(nullptr)
{ }

void TLock::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, State_);
    Save(context, Request_);
    // TODO(babenko): refactor when new serialization API is ready
    Save(context, TrunkNode_ ? TrunkNode_->GetId() : NObjectServer::NullObjectId);
    SaveObjectRef(context, Transaction_);
}

void TLock::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, State_);
    // COMPAT(babenko)
    if (context.GetVersion() == 24) {
        YCHECK(State_ == ELockState::Acquired);
    }
    Load(context, Request_);
    // TODO(babenko): refactor when new serialization API is ready
    auto trunkNodeId = Load<TNodeId>(context);
    TrunkNode_ = trunkNodeId == NObjectServer::NullObjectId ? nullptr : context.Get<TCypressNodeBase>(TVersionedNodeId(trunkNodeId));
    LoadObjectRef(context, Transaction_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

