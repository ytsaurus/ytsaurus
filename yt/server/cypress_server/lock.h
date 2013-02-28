#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

#include <cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
{
    TLockRequest(ELockMode mode)
        : Mode(mode)
    { }

    TLockRequest(ELockMode::EDomain mode)
        : Mode(mode)
    { }

    static TLockRequest SharedChild(const Stroka& key)
    {
        TLockRequest result(ELockMode::Shared);
        result.ChildKey = key;
        return result;
    }

    static TLockRequest SharedAttribute(const Stroka& key)
    {
        TLockRequest result(ELockMode::Shared);
        result.AttributeKey = key;
        return result;
    }

    ELockMode Mode;
    TNullable<Stroka> ChildKey;
    TNullable<Stroka> AttributeKey;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a lock held by a transaction of some Cypress node.
struct TLock
{
    ELockMode Mode;
    yhash_set<Stroka> ChildKeys;
    yhash_set<Stroka> AttributeKeys;
};

void Save(const NCellMaster::TSaveContext& context, const TLock& lock);
void Load(const NCellMaster::TLoadContext& context, TLock& lock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
