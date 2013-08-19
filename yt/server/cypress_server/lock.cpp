#include "stdafx.h"
#include "lock.h"

#include <ytlib/misc/serialize.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

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

void Save(NCellMaster::TSaveContext& context, const TLock& lock)
{
    Save(context, lock.Mode);
    Save(context, lock.ChildKeys);
    Save(context, lock.AttributeKeys);
}

void Load(NCellMaster::TLoadContext& context, TLock& lock)
{
    Load(context, lock.Mode);
    Load(context, lock.ChildKeys);
    Load(context, lock.AttributeKeys);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

