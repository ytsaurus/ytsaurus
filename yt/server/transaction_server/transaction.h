#pragma once

#include "public.h"

#include <ytlib/misc/property.h>
#include <ytlib/misc/ref_tracked.h>

#include <ytlib/cypress_client/public.h>

#include <server/cypress_server/public.h>

#include <server/chunk_server/public.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>

#include <server/security_server/public.h>
#include <server/security_server/cluster_resources.h>
#include <server/security_server/acl.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ETransactionState,
    (Active)
    (Committed)
    (Aborted)
);

class TTransaction
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTransaction>
{
    DEFINE_BYVAL_RW_PROPERTY(ETransactionState, State);
    DEFINE_BYVAL_RW_PROPERTY(TDuration, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, UncommittedAccountingEnabled);
    DEFINE_BYVAL_RW_PROPERTY(bool, StagedAccountingEnabled);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TTransaction*>, NestedTransactions);
    DEFINE_BYVAL_RW_PROPERTY(TTransaction*, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(yhash_set<NObjectServer::TObjectBase*>, StagedObjects);

    // Cypress stuff
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::TCypressNodeBase*>, LockedNodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::TCypressNodeBase*>, BranchedNodes);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NCypressServer::TCypressNodeBase*>, StagedNodes);

    // Security Manager stuff
    typedef yhash<NSecurityServer::TAccount*, NSecurityServer::TClusterResources> TAccountResourcesMap;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TTransaction(const TTransactionId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    bool IsActive() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
