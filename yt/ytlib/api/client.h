#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

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

#include <ytlib/new_table_client/public.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/security_client/public.h>

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

struct TGetTableInfoOptions
{ };

struct TMountTableOptions
    : public TTabletRangeOptions
{ };

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
    : public TTransactionalOptions
{ };

struct TCheckPermissionResult
{
    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    TNullable<Stroka> Subject;
};

struct TTransactionStartOptions
{
    TNullable<TDuration> Timeout;
    NTransactionClient::TTransactionId ParentId;
    bool AutoAbort = true;
    bool Ping = true;
    bool PingAncestors = true;
    NYTree::IAttributeDictionary* Attributes = nullptr;
};

struct TLookupRowsOptions
{
    NVersionedTableClient::TColumnFilter ColumnFilter;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::LastCommittedTimestamp;
    bool KeepMissingRows = false;
};

struct TSelectRowsOptions
{
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::LastCommittedTimestamp;
    //! If null then connection defaults are used.
    TNullable<i64> InputRowLimit;
    //! If null then connection defaults are used.
    TNullable<i64> OutputRowLimit;
};

struct TGetNodeOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    NYTree::IAttributeDictionary* Options = nullptr;
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
    bool IgnoreOpaque = false;
};

struct TSetNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TRemoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodesOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
};

struct TCreateNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    NYTree::IAttributeDictionary* Attributes = nullptr;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TLockNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    bool Waitable = false;
};

struct TCopyNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    bool PreserveAccount = false;
};

struct TMoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TLinkNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    NYTree::IAttributeDictionary* Attributes = nullptr;
    bool Recursive = false;
    bool IgnoreExisting = false;
};

struct TNodeExistsOptions
    : public TTransactionalOptions
{ };

struct TCreateObjectOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    NYTree::IAttributeDictionary* Attributes = nullptr;
};

struct TFileReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TNullable<i64> Offset;
    TNullable<i64> Length;
};

struct TFileWriterOptions
    : public TTransactionalOptions
{
    bool Append = true;
};

struct TJournalReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TNullable<i64> FirstRowIndex;
    TNullable<i64> RowCount;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
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
    // Transactions
    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;


    // Tables
    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRows(
        const NYPath::TYPath& path,
        NVersionedTableClient::TNameTablePtr nameTable,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<TErrorOr<NQueryClient::TQueryStatistics>> SelectRows(
        const Stroka& query,
        NVersionedTableClient::ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual TFuture<TErrorOr<std::pair<IRowsetPtr, NQueryClient::TQueryStatistics>>> SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    // TODO(babenko): batch read and batch write

    // Cypress
    virtual TFuture<TErrorOr<NYTree::TYsonString>> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = TGetNodeOptions()) = 0;

    virtual TFuture<TError> SetNode(
        const NYPath::TYPath& path,
        const NYTree::TYsonString& value,
        const TSetNodeOptions& options = TSetNodeOptions()) = 0;

    virtual TFuture<TError> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = TRemoveNodeOptions()) = 0;

    virtual TFuture<TErrorOr<NYTree::TYsonString>> ListNodes(
        const NYPath::TYPath& path,
        const TListNodesOptions& options = TListNodesOptions()) = 0;

    virtual TFuture<TErrorOr<NCypressClient::TNodeId>> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = TCreateNodeOptions()) = 0;

    virtual TFuture<TErrorOr<NCypressClient::TLockId>> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = TLockNodeOptions()) = 0;

    virtual TFuture<TErrorOr<NCypressClient::TNodeId>> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = TCopyNodeOptions()) = 0;

    virtual TFuture<TErrorOr<NCypressClient::TNodeId>> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = TMoveNodeOptions()) = 0;

    virtual TFuture<TErrorOr<NCypressClient::TNodeId>> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = TLinkNodeOptions()) = 0;

    virtual TFuture<TErrorOr<bool>> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = TNodeExistsOptions()) = 0;


    // Objects
    virtual TFuture<TErrorOr<NObjectClient::TObjectId>> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = TCreateObjectOptions()) = 0;


    // Files
    virtual IFileReaderPtr CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = TFileReaderOptions(),
        TFileReaderConfigPtr config = TFileReaderConfigPtr()) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath& path,
        const TFileWriterOptions& options = TFileWriterOptions(),
        TFileWriterConfigPtr config = TFileWriterConfigPtr()) = 0;


    // Journals
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = TJournalReaderOptions(),
        TJournalReaderConfigPtr config = TJournalReaderConfigPtr()) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = TJournalWriterOptions(),
        TJournalWriterConfigPtr config = TJournalWriterConfigPtr()) = 0;

    // TODO(babenko): scheduler commands

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
    virtual IConnectionPtr GetConnection() = 0;
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    //! Returns a async flag indicating completion.
    virtual TFuture<void> Terminate() = 0;


    // Tables
    virtual TAsyncError MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    virtual TAsyncError UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    virtual TAsyncError RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = TRemountTableOptions()) = 0;

    virtual TAsyncError ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;


    // Security
    virtual TAsyncError AddMember(
        const Stroka& group,
        const Stroka& member,
        const TAddMemberOptions& options = TAddMemberOptions()) = 0;

    virtual TAsyncError RemoveMember(
        const Stroka& group,
        const Stroka& member,
        const TRemoveMemberOptions& options = TRemoveMemberOptions()) = 0;

    virtual TFuture<TErrorOr<TCheckPermissionResult>> CheckPermission(
        const Stroka& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = TCheckPermissionOptions()) = 0;

};

DEFINE_REFCOUNTED_TYPE(IClient)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

