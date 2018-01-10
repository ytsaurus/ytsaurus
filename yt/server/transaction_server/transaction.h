#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/security_server/acl.h>
#include <yt/server/security_server/cluster_resources.h>
#include <yt/server/security_server/public.h>

#include <yt/server/hive/transaction_detail.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NHiveServer::TTransactionBase<NObjectServer::TNonversionedObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(bool, AccountingEnabled);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TString>, Title);
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagList, SecondaryCellTags);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransaction*>, NestedTransactions);
    DEFINE_BYVAL_RW_PROPERTY(TTransaction*, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NObjectServer::TObjectBase*>, StagedObjects);

    struct TExportEntry
    {
        NObjectServer::TObjectBase* Object;
        NObjectClient::TCellTag DestinationCellTag;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TExportEntry>, ExportedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NObjectServer::TObjectBase*>, ImportedObjects);

    // Cypress stuff
    typedef THashSet<NCypressServer::TCypressNodeBase*> TLockedNodeSet;
    DEFINE_BYREF_RW_PROPERTY(TLockedNodeSet, LockedNodes);
    typedef THashSet<NCypressServer::TLock*> TLockSet;
    DEFINE_BYREF_RW_PROPERTY(TLockSet, Locks);
    typedef std::vector<NCypressServer::TCypressNodeBase*> TBranchedNodeList;
    DEFINE_BYREF_RW_PROPERTY(TBranchedNodeList, BranchedNodes);
    typedef std::vector<NCypressServer::TCypressNodeBase*> TStagedNodeList;
    DEFINE_BYREF_RW_PROPERTY(TStagedNodeList, StagedNodes);

    // Security Manager stuff
    typedef THashMap<NSecurityServer::TAccount*, NSecurityServer::TClusterResources> TAccountResourcesMap;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TTransaction(const TTransactionId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NYson::TYsonString GetErrorDescription() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
