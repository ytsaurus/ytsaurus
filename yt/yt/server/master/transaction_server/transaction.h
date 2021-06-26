#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/security_server/acl.h>
#include <yt/yt/server/master/security_server/cluster_resources.h>
#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/lib/hive/transaction_detail.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

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
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagList, ExternalizedToCellTags);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransaction*>, NestedTransactions);
    DEFINE_BYVAL_RW_PROPERTY(TTransaction*, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NObjectServer::TObject*>, StagedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTransaction*>, PrerequisiteTransactions);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransaction*>, DependentTransactions);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, Deadline);
    DEFINE_BYVAL_RW_PROPERTY(int, Depth);
    // Only set when transaction commit is in progress that has been initiated via an Hive message.
    // There's no strong reason for this field to be persistent, but it may ease future debugging.
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, NativeCommitMutationRevision, NHydra::NullRevision);

    struct TExportEntry
    {
        NObjectServer::TObject* Object;
        NObjectClient::TCellTag DestinationCellTag;

        void Persist(const NCellMaster::TPersistenceContext& context);
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TExportEntry>, ExportedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NObjectServer::TObject*>, ImportedObjects);

    // Cypress stuff
    using TLockedNodeSet = THashSet<NCypressServer::TCypressNode*>;
    DEFINE_BYREF_RW_PROPERTY(TLockedNodeSet, LockedNodes);
    using TLockSet = THashSet<NCypressServer::TLock*>;
    DEFINE_BYREF_RW_PROPERTY(TLockSet, Locks);
    using TBranchedNodeList = std::vector<NCypressServer::TCypressNode*>;
    DEFINE_BYREF_RW_PROPERTY(TBranchedNodeList, BranchedNodes);
    using TStagedNodeList = std::vector<NCypressServer::TCypressNode*>;
    DEFINE_BYREF_RW_PROPERTY(TStagedNodeList, StagedNodes);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNode*>, LockedDynamicTables);

    // Security Manager stuff
    using TAccountResourcesMap = THashMap<NSecurityServer::TAccount*, NSecurityServer::TClusterResources>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);
    
    using TAccountResourceUsageLeaseSet = THashSet<NSecurityServer::TAccountResourceUsageLease*>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourceUsageLeaseSet, AccountResourceUsageLeases);

public:
    explicit TTransaction(TTransactionId id, bool upload = false);

    bool IsUpload() const;

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NYson::TYsonString GetErrorDescription() const;

    //! Walks up parent links and returns the topmost ancestor.
    /*!
     *  NB: complexity is O(number of intermediate ancestors).
     */
    TTransaction* GetTopmostTransaction();
    const TTransaction* GetTopmostTransaction() const;

    //! Returns |true| if this transaction has been replicated to the specified cell.
    bool IsReplicatedToCell(NObjectClient::TCellTag cellTag) const;

    //! Returns |true| if this transaction has been externalized to the specified cell.
    bool IsExternalizedToCell(NObjectClient::TCellTag cellTag) const;

    //! Returns |true| if this a (topmost or nested) externalized transaction.
    bool IsExternalized() const;

    //! For externalized transactions only; returns the original transaction id.
    TTransactionId GetOriginalTransactionId() const;

private:
    bool Upload_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
