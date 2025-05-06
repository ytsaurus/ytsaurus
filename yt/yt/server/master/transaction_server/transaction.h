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

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>

#include <optional>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TBranchedNodeSet
{
public:
    [[nodiscard]] int Size() const noexcept;
    [[nodiscard]] bool Empty() const noexcept;
    void Clear();

    using TVector = std::vector<NCypressServer::TCypressNodeRawPtr>;
    using TIterator = typename TVector::const_iterator;
    TIterator begin() const noexcept;
    TIterator end() const noexcept;

    //! Returns any node. If set is empty behavior is undefined.
    NCypressServer::TCypressNode* GetAnyNode();

    void InsertOrCrash(NCypressServer::TCypressNode* node);

    // NB: Erasure of node obtained with |GetAnyNode()| is a bit faster.
    void EraseOrCrash(NCypressServer::TCypressNode* node);

    void Persist(const NCellMaster::TPersistenceContext& context);

private:
    // NB: looks like flat_hash_map is a bit faster than THashMap in this case (5-10%).
    using TMap = absl::flat_hash_map<
        NCypressServer::TCypressNodeRawPtr,
        int,
        THash<NCypressServer::TCypressNodeRawPtr>
    >;

    TVector Nodes_;
    TMap NodeToIndex_;
};

////////////////////////////////////////////////////////////////////////////////

class TBulkInsertState
{
    using TCellTagToLockableTables = THashMap<NObjectClient::TCellTag, std::vector<NTableClient::TTableId>>;

public:
    explicit TBulkInsertState(TTransaction* transaction);

    // LockableDynamicTables used only on coordinator to ensure, that dynamic tables were locked before commit
    // and throw NeedLockDynamicTablesBeforeCommit if not.
    DEFINE_BYREF_RW_PROPERTY(TCellTagToLockableTables, LockableDynamicTables);
    // LockedDynamicTables used only on external cell to lock/unlock dynamic tables.
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNodeRawPtr>, LockedDynamicTables);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TTransactionId>, DescendantTransactionsPresentedInTimestampHolder);

    void OnTransactionCommitted(bool isTimestampPresentInHolder);
    void OnTransactionAborted();

    bool HasConflict(NTableServer::TTableId tableId);
    void AddLockableDynamicTables(
        std::vector<std::pair<NObjectClient::TCellTag, std::vector<NTableClient::TTableId>>>&& lockableDynamicTables);

    void Persist(const NCellMaster::TPersistenceContext& context);

private:
    // Used for conflict resolving when transaction want to take lock on dynamic table.
    // Non-empty only for topmost transactions.
    THashSet<NTableServer::TTableId> AllTablesLockedByDescendantTransactions_;

    TTransaction* Transaction_ = nullptr;
    TTransaction* TopmostTransaction_ = nullptr;

    void FindTopmostTransaction();

    template <class TLockableDynamicTables>
    void AddToLockableDynamicTables(TLockableDynamicTables&& lockableDynamicTables);
};

////////////////////////////////////////////////////////////////////////////////

class TTransaction
    : public NTransactionSupervisor::TTransactionBase<NObjectServer::TObject>
    , public TRefTracked<TTransaction>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, Timeout);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<std::string>, Title);
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagList, ReplicatedToCellTags);
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagList, ExternalizedToCellTags);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransactionRawPtr>, NestedTransactions);
    DEFINE_BYVAL_RW_PROPERTY(TTransactionRawPtr, Parent);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NObjectServer::TObjectRawPtr>, StagedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<TTransactionRawPtr>, PrerequisiteTransactions);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTransactionRawPtr>, DependentTransactions);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, Deadline);
    DEFINE_BYVAL_RW_PROPERTY(int, Depth);
    // Only set when transaction commit is in progress that has been initiated via an Hive message.
    // There's no strong reason for this field to be persistent, but it may ease future debugging.
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, NativeCommitMutationRevision, NHydra::NullRevision);

    // COMPAT(h0pless): Remove this when all issues with system transaction types will be ironed out.
    DEFINE_BYVAL_RW_PROPERTY(bool, IsCypressTransaction);

    // COMPAT(kvk1920)
    // NB: meaningful only for Cypress tx.
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(NativeTxExternalizationEnabled);

    DEFINE_BYREF_RW_PROPERTY(THashSet<NHydra::TCellId>, LeaseCellIds);

    DEFINE_BYREF_RW_PROPERTY(TPromise<void>, LeasesRevokedPromise);

    struct TExportEntry
    {
        NObjectServer::TObjectRawPtr Object;
        NObjectClient::TCellTag DestinationCellTag;

        void Persist(const NCellMaster::TPersistenceContext& context);
    };

    DEFINE_BYREF_RW_PROPERTY(std::vector<TExportEntry>, ExportedObjects);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NObjectServer::TObjectRawPtr>, ImportedObjects);

    // Cypress stuff.
    using TLockedNodeSet = THashSet<NCypressServer::TCypressNodeRawPtr>;
    DEFINE_BYREF_RW_PROPERTY(TLockedNodeSet, LockedNodes);
    using TLockSet = THashSet<NCypressServer::TLockRawPtr>;
    DEFINE_BYREF_RO_PROPERTY(TLockSet, Locks);
    DEFINE_BYREF_RW_PROPERTY(TBranchedNodeSet, BranchedNodes);
    using TStagedNodeList = std::vector<NCypressServer::TCypressNodeRawPtr>;
    DEFINE_BYREF_RW_PROPERTY(TStagedNodeList, StagedNodes);
    DEFINE_BYREF_RW_PROPERTY(TBulkInsertState, BulkInsertState, this);
    DEFINE_BYREF_RW_PROPERTY(THashSet<NTableServer::TTableNodeRawPtr>, TablesWithBackupCheckpoints);

    // Security Manager stuff.
    using TAccountResourcesMap = THashMap<NSecurityServer::TAccountPtr, NSecurityServer::TClusterResources>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourcesMap, AccountResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    using TAccountResourceUsageLeaseSet = THashSet<NSecurityServer::TAccountResourceUsageLeaseRawPtr>;
    DEFINE_BYREF_RW_PROPERTY(TAccountResourceUsageLeaseSet, AccountResourceUsageLeases);

    // Sequoia stuff.
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(SequoiaTransaction, false);
    DEFINE_BYREF_RW_PROPERTY(NSequoiaClient::NProto::TWriteSet, SequoiaWriteSet);
    DEFINE_BYVAL_RW_PROPERTY(NRpc::TAuthenticationIdentity, AuthenticationIdentity, NRpc::GetRootAuthenticationIdentity());

    // NB: This field is transient.
    DEFINE_BYVAL_RW_PROPERTY(NTracing::TTraceContextPtr, TraceContext);

public:
    using TTransactionBase::TTransactionBase;
    explicit TTransaction(TTransactionId id, bool upload = false);

    bool IsUpload() const;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    NYson::TYsonString GetErrorDescription() const;

    //! Walks up parent links and returns the topmost ancestor.
    /*!
     *  NB: Complexity is O(number of intermediate ancestors).
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

    //! Can be confused with IsSequiaTransaction().
    bool IsSequoia() const = delete;

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
