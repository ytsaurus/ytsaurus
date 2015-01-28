#pragma once

#include "public.h"
#include "connection.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <core/actions/future.h>

#include <core/ytree/yson_string.h>
#include <core/ytree/attribute_provider.h>
#include <core/ytree/permission.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/query_client/public.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/new_table_client/row_base.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/security_client/public.h>

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TTabletRangeOptions
{
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;
};

struct TTransactionalOptions
{
    //! Ignored when queried via transaction.
    NObjectClient::TTransactionId TransactionId;
    bool PingAncestors = false;
};

struct TSuppressableAccessTrackingOptions
{
    bool SuppressAccessTracking = false;
};

struct TMutatingOptions
{
    NHydra::TMutationId MutationId;
};

struct TReadOnlyOptions
{
    EMasterChannelKind ReadFrom = EMasterChannelKind::LeaderOrFollower;
};

struct TPrerequisiteOptions
{
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;
};

struct TGetTableInfoOptions
{ };

struct TMountTableOptions
    : public TTabletRangeOptions
{
    NTabletClient::TTabletCellId CellId;
};

struct TUnmountTableOptions
    : public TTabletRangeOptions
{
    bool Force = false;
};

struct TRemountTableOptions
    : public TTabletRangeOptions
{ };

struct TReshardTableOptions
    : public TTabletRangeOptions
{ };

struct TAddMemberOptions
    : public TMutatingOptions
{ };

struct TRemoveMemberOptions
    : public TMutatingOptions
{ };

struct TCheckPermissionOptions
    : public TReadOnlyOptions
    , public TTransactionalOptions
{ };

struct TCheckPermissionResult
{
    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    TNullable<Stroka> Subject;
};

struct TTransactionStartOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
{
    TNullable<TDuration> Timeout;
    NTransactionClient::TTransactionId ParentId;
    bool AutoAbort = true;
    bool Ping = true;
    bool PingAncestors = true;
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
};

struct TTransactionCommitOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TTransactionAbortOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Force = false;
};

struct TLookupRowsOptions
{
    NVersionedTableClient::TColumnFilter ColumnFilter;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    bool KeepMissingRows = false;
};

struct TSelectRowsOptions
{
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    //! If null then connection defaults are used.
    TNullable<i64> InputRowLimit;
    //! If null then connection defaults are used.
    TNullable<i64> OutputRowLimit;
    //! If |true| then incomplete result would lead to a failure.
    bool FailOnIncompleteResult = true;
};

struct TGetNodeOptions
    : public TTransactionalOptions
    , public TReadOnlyOptions
    , public TSuppressableAccessTrackingOptions
{
    // XXX(sandello): This one is used only in ProfileManager to pass `from_time`.
    std::shared_ptr<const NYTree::IAttributeDictionary> Options;
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
    bool IgnoreOpaque = false;
};

struct TSetNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodeOptions
    : public TTransactionalOptions
    , public TReadOnlyOptions
    , public TSuppressableAccessTrackingOptions
{
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
};

struct TCreateNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TLockNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Waitable = false;
};

struct TCopyNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool PreserveAccount = false;
};

struct TMoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool PreserveAccount = true;
};

struct TLinkNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    //! Attributes of a newly created link node.
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TNodeExistsOptions
    : public TReadOnlyOptions
    , public TTransactionalOptions
{ };

struct TCreateObjectOptions
    : public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
};

struct TFileReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TNullable<i64> Offset;
    TNullable<i64> Length;
    TFileReaderConfigPtr Config;
};

struct TFileWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    bool Append = true;
    TFileWriterConfigPtr Config;
};

struct TJournalReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TNullable<i64> FirstRowIndex;
    TNullable<i64> RowCount;
    TJournalReaderConfigPtr Config;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    TJournalWriterConfigPtr Config;
};

struct TStartOperationOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{ };

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic set of functions that can be invoked
//! both standalone and inside transaction.
/*
 *  This interface contains methods shared by IClient and ITransaction.
 *  
 *  Thread affinity: single
 */
struct IClientBase
    : public virtual TRefCounted
{
    virtual IConnectionPtr GetConnection() = 0;


    // Transactions
    virtual TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;


    // Tables
    virtual TFuture<IRowsetPtr> LookupRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<IRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<NQueryClient::TQueryStatistics> SelectRows(
        const Stroka& query,
        NVersionedTableClient::ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual TFuture<std::pair<IRowsetPtr, NQueryClient::TQueryStatistics>> SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    // TODO(babenko): batch read and batch write

    // Cypress
    virtual TFuture<NYTree::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = TGetNodeOptions()) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYTree::TYsonString& value,
        const TSetNodeOptions& options = TSetNodeOptions()) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = TRemoveNodeOptions()) = 0;

    virtual TFuture<NYTree::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = TListNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = TCreateNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TLockId> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = TLockNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = TCopyNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = TMoveNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = TLinkNodeOptions()) = 0;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = TNodeExistsOptions()) = 0;


    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = TCreateObjectOptions()) = 0;


    // Files
    virtual IFileReaderPtr CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = TFileReaderOptions()) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath& path,
        const TFileWriterOptions& options = TFileWriterOptions()) = 0;


    // Journals
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = TJournalReaderOptions()) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = TJournalWriterOptions()) = 0;

};

DEFINE_REFCOUNTED_TYPE(IClientBase)

///////////////////////////////////////////////////////////////////////////////

//! A central entry point for all interactions with the YT cluster.
/*!
 *  In contrast to IConnection, each IClient represents an authenticated entity.
 *  The needed username is passed to #IConnection::CreateClient via options.
 *  Note that YT API has no built-in authentication mechanisms so it must be wrapped
 *  with appropriate logic.
 *  
 *  Most methods accept |TransactionId| as a part of their options.
 *  A similar effect can be achieved by issuing requests via ITransaction.
 *  
 */
struct IClient
    : public IClientBase
{
    // TODO(babenko): consider hiding these guys
    virtual NRpc::IChannelPtr GetMasterChannel(EMasterChannelKind kind) = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NRpc::IChannelFactoryPtr GetNodeChannelFactory() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    //! Returns a async flag indicating completion.
    virtual TFuture<void> Terminate() = 0;


    // Tables
    virtual TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = TRemountTableOptions()) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;


    // Security
    virtual TFuture<void> AddMember(
        const Stroka& group,
        const Stroka& member,
        const TAddMemberOptions& options = TAddMemberOptions()) = 0;

    virtual TFuture<void> RemoveMember(
        const Stroka& group,
        const Stroka& member,
        const TRemoveMemberOptions& options = TRemoveMemberOptions()) = 0;

    virtual TFuture<TCheckPermissionResult> CheckPermission(
        const Stroka& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = TCheckPermissionOptions()) = 0;


    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYTree::TYsonString& spec,
        const TStartOperationOptions& options = TStartOperationOptions()) = 0;

    virtual TFuture<void> AbortOperation(const NScheduler::TOperationId& operationId) = 0;

    virtual TFuture<void> SuspendOperation(const NScheduler::TOperationId& operationId) = 0;

    virtual TFuture<void> ResumeOperation(const NScheduler::TOperationId& operationId) = 0;


};

DEFINE_REFCOUNTED_TYPE(IClient)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

