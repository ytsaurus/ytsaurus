#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/security_server/acl.h>
#include <yt/server/master/security_server/cluster_resources.h>
#include <yt/server/master/security_server/public.h>

#include <yt/server/master/table_server/public.h>

#include <yt/server/lib/hive/transaction_detail.h>

#include <yt/ytlib/cypress_client/public.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NHiveServer::TTransactionBase<NObjectServer::TNonversionedObjectBase>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, Title);
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagList, ReplicatedToCellTags);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransaction*>, NestedNativeTransactions);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransactionId>, NestedExternalTransactionIds);
    DEFINE_BYVAL_RW_PROPERTY(bool, UnregisterFromParentOnCommit);
    DEFINE_BYVAL_RW_PROPERTY(bool, UnregisterFromParentOnAbort);
    DEFINE_BYVAL_RW_PROPERTY(TTransaction*, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NObjectServer::TObject*>, StagedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTransaction*>, PrerequisiteTransactions);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransaction*>, DependentTransactions);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, Deadline);
    DEFINE_BYVAL_RW_PROPERTY(int, Depth);

    struct TExportEntry
    {
        NObjectServer::TObject* Object;
        NObjectClient::TCellTag DestinationCellTag;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TExportEntry>, ExportedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NObjectServer::TObject*>, ImportedObjects);

    // Cypress stuff
    typedef THashSet<NCypressServer::TCypressNode*> TLockedNodeSet;
    DEFINE_BYREF_RW_PROPERTY(TLockedNodeSet, LockedNodes);
    typedef THashSet<NCypressServer::TLock*> TLockSet;
    DEFINE_BYREF_RW_PROPERTY(TLockSet, Locks);
    typedef std::vector<NCypressServer::TCypressNode*> TBranchedNodeList;
    DEFINE_BYREF_RW_PROPERTY(TBranchedNodeList, BranchedNodes);
    typedef std::vector<NCypressServer::TCypressNode*> TStagedNodeList;
    DEFINE_BYREF_RW_PROPERTY(TStagedNodeList, StagedNodes);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNode*>, LockedDynamicTables);

    // Security Manager stuff
    typedef THashMap<NSecurityServer::TAccount*, NSecurityServer::TClusterResources> TAccountResourcesMap;
    // XXX(babenko): weak ref to TAccount?
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TTransaction(TTransactionId id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void RecomputeResourceUsage();

    NYson::TYsonString GetErrorDescription() const;

    //! Returns true iff this transaction is (arbitrarily deeply) nested in #transaction.
    /*!
     *  A transaction is NOT a descendant of itself.
     *  NB: complexity is O(number of intermediate descendants).
     */
    bool IsDescendantOf(TTransaction* transaction) const;

private:
    void AddNodeResourceUsage(const NCypressServer::TCypressNode* node, bool staged);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
