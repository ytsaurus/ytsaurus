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

#include <yt/yt/server/lib/transaction_supervisor/transaction_detail.h>

#include <yt/yt/ytlib/cypress_client/public.h>

#include <yt/yt/ytlib/sequoia_client/write_set.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <optional>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NTransactionSupervisor::TTransactionBase<NObjectServer::TObject>
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

    // COMPAT(h0pless): Remove this when all issues with system transaction types will be ironed out.
    DEFINE_BYVAL_RW_PROPERTY(bool, IsCypressTransaction);

    DEFINE_BYREF_RW_PROPERTY(THashSet<NHydra::TCellId>, LeaseCellIds);

    DEFINE_BYREF_RW_PROPERTY(TPromise<void>, LeasesRevokedPromise);

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
    DEFINE_BYREF_RO_PROPERTY(TLockSet, Locks);
    using TBranchedNodeList = std::vector<NCypressServer::TCypressNode*>;
    DEFINE_BYREF_RW_PROPERTY(TBranchedNodeList, BranchedNodes);
    using TStagedNodeList = std::vector<NCypressServer::TCypressNode*>;
    DEFINE_BYREF_RW_PROPERTY(TStagedNodeList, StagedNodes);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNode*>, LockedDynamicTables);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNode*>, TablesWithBackupCheckpoints);

    // Security Manager stuff
    using TAccountResourcesMap = THashMap<NSecurityServer::TAccountPtr, NSecurityServer::TClusterResources>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    using TAccountResourceUsageLeaseSet = THashSet<NSecurityServer::TAccountResourceUsageLease*>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourceUsageLeaseSet, AccountResourceUsageLeases);

    // Sequoia stuff.
    DEFINE_BYVAL_RW_PROPERTY(bool, IsSequoiaTransaction, false);
    DEFINE_BYREF_RW_PROPERTY(NSequoiaClient::NProto::TWriteSet, SequoiaWriteSet);
    DEFINE_BYVAL_RW_PROPERTY(NRpc::TAuthenticationIdentity, AuthenticationIdentity, NRpc::GetRootAuthenticationIdentity());

public:
    using TTransactionBase::TTransactionBase;
    explicit TTransaction(TTransactionId id, bool upload = false);

    bool IsUpload() const;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

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

    //! Returns total number of locks taken by transaction and it's children.
    int GetRecursiveLockCount() const;

    // COMPAT(h0pless)
    void IncreaseRecursiveLockCount(int delta = 1);

    void AttachLock(NCypressServer::TLock* lock, const NObjectServer::IObjectManagerPtr& objectManager);
    void DetachLock(
        NCypressServer::TLock* lock,
        const NObjectServer::IObjectManagerPtr& objectManager,
        bool resetLockTransaction = true);

    //! For externalized transactions only; returns the original transaction id.
    TTransactionId GetOriginalTransactionId() const;

    void SetTransactionLeasesState(ETransactionLeasesState newState);
    ETransactionLeasesState GetTransactionLeasesState() const;

    void SetSuccessorTransactionLeaseCount(int newLeaseCount);
    int GetSuccessorTransactionLeaseCount() const;

private:
    void IncrementRecursiveLockCount();
    void DecrementRecursiveLockCount();

    bool Upload_ = false;
    int RecursiveLockCount_ = 0;

    int SuccessorTransactionLeaseCount_ = 0;

    ETransactionLeasesState LeasesState_ = ETransactionLeasesState::Active;
};

DEFINE_MASTER_OBJECT_TYPE(TTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
