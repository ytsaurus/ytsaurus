#pragma once

#include "public.h"
#include "connection.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <core/actions/future.h>

#include <core/yson/string.h>

#include <core/ytree/ypath_service.h>
#include <core/ytree/permission.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/query_client/public.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/table_client/row_base.h>
#include <ytlib/table_client/config.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/security_client/public.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/job_tracker_client/public.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TTimeoutOptions
{
    TNullable<TDuration> Timeout;
};

struct TTabletRangeOptions
{
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;
};

struct TTransactionalOptions
{
    //! Ignored when queried via transaction.
    NObjectClient::TTransactionId TransactionId;
    bool Ping = false;
    bool PingAncestors = false;
};

struct TSuppressableAccessTrackingOptions
{
    bool SuppressAccessTracking = false;
    bool SuppressModificationTracking = false;
};

struct TMutatingOptions
{
    NRpc::TMutationId MutationId;
    bool Retry = false;
};

struct TReadOnlyOptions
{
    EMasterChannelKind ReadFrom = EMasterChannelKind::LeaderOrFollower;
};

struct TPrerequisiteOptions
{
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;
};

struct TMountTableOptions
    : public TTimeoutOptions
    , public TTabletRangeOptions
{
    NTabletClient::TTabletCellId CellId;
    //! A lower estimate for the table's uncompressed size.
    //! Used for balancing tablets across tablet cells.
    //! Default is 1 Tb.
    i64 EstimatedUncompressedSize = (i64) 1 * 1024 * 1024 * 1024 * 1024;
    //! Same as above but for compressed size.
    //! Default is 100 Gb.
    i64 EstimatedCompressedSize = (i64) 100 * 1024 * 1024 * 1024;
};

struct TUnmountTableOptions
    : public TTimeoutOptions
    , public TTabletRangeOptions
{
    bool Force = false;
};

struct TRemountTableOptions
    : public TTimeoutOptions
    , public TTabletRangeOptions
{ };

struct TReshardTableOptions
    : public TTimeoutOptions
    , public TTabletRangeOptions
{ };

struct TAddMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TRemoveMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TCheckPermissionOptions
    : public TTimeoutOptions
    , public TReadOnlyOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{ };

struct TCheckPermissionResult
{
    TError ToError(const Stroka& user, NYTree::EPermission permission) const;

    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    TNullable<Stroka> ObjectName;
    NSecurityClient::TSubjectId SubjectId;
    TNullable<Stroka> SubjectName;
};

struct TTransactionStartOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
{
    TNullable<TDuration> Timeout;
    NTransactionClient::TTransactionId ParentId;
    bool AutoAbort = true;
    TNullable<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = true;
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;
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
    : public TTimeoutOptions
{
    NTableClient::TColumnFilter ColumnFilter;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    bool KeepMissingRows = false;
};

struct TSelectRowsOptions
    : public TTimeoutOptions
{
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    //! If null then connection defaults are used.
    TNullable<i64> InputRowLimit;
    //! If null then connection defaults are used.
    TNullable<i64> OutputRowLimit;
    //! Limits range expanding.
    ui64 RangeExpansionLimit = 1000;
    //! If |true| then incomplete result would lead to a failure.
    bool FailOnIncompleteResult = true;
    //! If |true| then logging is more verbose.
    bool VerboseLogging = false;
    //! Limits maximum parallel subqueries.
    ui64 MaxSubqueries = 0;
    //! Enables generated code caching.
    bool EnableCodeCache = true;
};

struct TGetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TReadOnlyOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    // XXX(sandello): This one is used only in ProfileManager to pass `from_time`.
    std::shared_ptr<const NYTree::IAttributeDictionary> Options;
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
    bool IgnoreOpaque = false;
};

struct TSetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TReadOnlyOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
};

struct TCreateNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TLockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Waitable = false;
    TNullable<Stroka> ChildKey;
    TNullable<Stroka> AttributeKey;
};

struct TCopyNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
};

struct TMoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = true;
};

struct TLinkNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    //! Attributes of a newly created link node.
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TNodeExistsOptions
    : public TTimeoutOptions
    , public TReadOnlyOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{ };

struct TCreateObjectOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
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

struct TTableReaderOptions
    : public TTransactionalOptions
{
    bool Unordered = false;
    NTableClient::TTableReaderConfigPtr Config;
    NChunkClient::TRemoteReaderOptionsPtr RemoteReaderOptions;
};

struct TStartOperationOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TAbortOperationOptions
    : public TTimeoutOptions
{ };

struct TSuspendOperationOptions
    : public TTimeoutOptions
{ };

struct TResumeOperationOptions
    : public TTimeoutOptions
{ };

struct TDumpJobContextOptions
    : public TTimeoutOptions
{ };

struct TStraceJobOptions
    : public TTimeoutOptions
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
        NTableClient::TNameTablePtr nameTable,
        NTableClient::TKey key,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<IRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const std::vector<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<NQueryClient::TQueryStatistics> SelectRows(
        const Stroka& query,
        NTableClient::ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual TFuture<std::pair<IRowsetPtr, NQueryClient::TQueryStatistics>> SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    // TODO(babenko): batch read and batch write

    // Cypress
    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = TGetNodeOptions()) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = TSetNodeOptions()) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = TRemoveNodeOptions()) = 0;

    virtual TFuture<NYson::TYsonString> ListNode(
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


    // Tables
    virtual NTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options) = 0;
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
    virtual NRpc::IChannelPtr GetMasterChannel(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NRpc::IChannelFactoryPtr GetNodeChannelFactory() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;
    virtual NQueryClient::IExecutorPtr GetQueryExecutor() = 0;

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
        const std::vector<NTableClient::TKey>& pivotKeys,
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
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = TStartOperationOptions()) = 0;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationId& operationId,
        const TAbortOperationOptions& options = TAbortOperationOptions()) = 0;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationId& operationId,
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationId& operationId,
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId& jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = TDumpJobContextOptions()) = 0;

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId& jobId,
        const TStraceJobOptions& options = TStraceJobOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

