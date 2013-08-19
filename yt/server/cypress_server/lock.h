#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
{
    TLockRequest(ELockMode mode);
    TLockRequest(ELockMode::EDomain mode);

    static TLockRequest SharedChild(const Stroka& key);
    static TLockRequest SharedAttribute(const Stroka& key);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

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

void Save(NCellMaster::TSaveContext& context, const TLock& lock);
void Load(NCellMaster::TLoadContext& context, TLock& lock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
