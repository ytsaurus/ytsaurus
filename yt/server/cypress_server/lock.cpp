#include "stdafx.h"
#include "lock.h"

#include <core/misc/serialize.h>

#include <server/transaction_server/transaction.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NCypressServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TLockRequest::TLockRequest()
{ }

TLockRequest::TLockRequest(ELockMode mode)
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

void TLockRequest::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Mode);
    Save(context, ChildKey);
    Save(context, AttributeKey);
}

void TLockRequest::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Mode);
    Load(context, ChildKey);
    Load(context, AttributeKey);
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionLockState::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Mode);
    Save(context, ChildKeys);
    Save(context, AttributeKeys);
}

void TTransactionLockState::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Mode);
    Load(context, ChildKeys);
    Load(context, AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(const TLockId& id)
    : TNonversionedObjectBase(id)
    , State_(ELockState::Pending)
    , TrunkNode_(nullptr)
    , Transaction_(nullptr)
{ }

void TLock::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, State_);
    Save(context, Request_);
    TNonversionedObjectRefSerializer::Save(context, TrunkNode_);
    Save(context, Transaction_);
}

void TLock::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, State_);
    Load(context, Request_);
    TNonversionedObjectRefSerializer::Load(context, TrunkNode_);
    Load(context, Transaction_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

