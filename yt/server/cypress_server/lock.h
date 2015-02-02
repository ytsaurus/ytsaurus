#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>

#include <core/actions/signal.h>

#include <server/object_server/object_detail.h>

#include <server/transaction_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TLockRequest
{
    TLockRequest();
    TLockRequest(ELockMode mode);

    static TLockRequest SharedChild(const Stroka& key);
    static TLockRequest SharedAttribute(const Stroka& key);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    ELockMode Mode;
    TNullable<Stroka> ChildKey;
    TNullable<Stroka> AttributeKey;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes all locks held by a transaction of some Cypress node.
struct TTransactionLockState
{
    ELockMode Mode;
    yhash_set<Stroka> ChildKeys;
    yhash_set<Stroka> AttributeKeys;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a lock (either held or waiting).
class TLock
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TLock>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ELockState, State);
    DEFINE_BYREF_RW_PROPERTY(TLockRequest, Request);
    DEFINE_BYVAL_RW_PROPERTY(TCypressNodeBase*, TrunkNode);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);
    
    // Not persisted.
    typedef std::list<TLock*>::iterator TLockListIterator;
    DEFINE_BYVAL_RW_PROPERTY(TLockListIterator, LockListIterator);

public:
    explicit TLock(const TLockId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
