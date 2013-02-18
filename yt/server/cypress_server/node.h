#pragma once

#include "public.h"
#include "lock.h"

#include <ytlib/misc/property.h>

#include <ytlib/cypress_client/public.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

#include <server/security_server/cluster_resources.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a common base for all versioned (aka Cypress) nodes.
class TCypressNodeBase
    : public NObjectServer::TObjectBase
{
public:
    typedef yhash_map<NTransactionServer::TTransaction*, TLock> TLockMap;
    DEFINE_BYREF_RW_PROPERTY(TLockMap, Locks);

    typedef yhash_set<TCypressNodeBase*> TNodeSet;
    //! Contains all nodes with parent pointing here.
    //! When a node dies parent pointers of its immediate ancestors are reset.
    DEFINE_BYREF_RW_PROPERTY(TNodeSet, ImmediateAncestors);

    DEFINE_BYVAL_RW_PROPERTY(ELockMode, LockMode);

    DEFINE_BYVAL_RW_PROPERTY(TCypressNodeBase*, TrunkNode);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, Transaction);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, ModificationTime);

    DEFINE_BYVAL_RW_PROPERTY(NSecurityServer::TAccount*, Account);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TClusterResources, CachedResourceUsage);

    explicit TCypressNodeBase(const TVersionedNodeId& id);
    virtual ~TCypressNodeBase();

    TCypressNodeBase* GetParent() const;
    void SetParent(TCypressNodeBase* newParent);
    void ResetParent();

    //! Returns the composite (versioned) id of the node.
    TVersionedNodeId GetVersionedId() const;

    virtual int GetOwningReplicationFactor() const;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const;

    // Similar methods are also declared in TObjectBase but starting from TCypressNodeBase
    // they become virtual.
    virtual void Save(const NCellMaster::TSaveContext& context) const;
    virtual void Load(const NCellMaster::TLoadContext& context);

private:
    TCypressNodeBase* Parent_;
    NTransactionServer::TTransactionId TransactionId;

};

NObjectServer::TVersionedObjectId GetObjectId(const TCypressNodeBase* object);
bool CompareObjectsForSerialization(const TCypressNodeBase* lhs, const TCypressNodeBase* rhs);

//////////////////////////////////////////////////////////////////////////////// 

} // namespace NCypressServer
} // namespace NYT
