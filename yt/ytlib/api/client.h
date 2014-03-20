#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <core/ytree/yson_string.h>
#include <core/ytree/attribute_provider.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TClientOptions
{
    TNullable<Stroka> User;
};

struct TTabletRangeOptions
{
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;
};

struct TTransactionalOptions
{
    TTransactionalOptions()
        : PingAncestors(false)
    { }

    //! Ignored when queried via transaction.
    NObjectClient::TTransactionId TransactionId;
    bool PingAncestors;
};

struct TSuppressableAccessTrackingOptions
{
    TSuppressableAccessTrackingOptions()
        : SuppressAccessTracking(false)
    { }

    bool SuppressAccessTracking;
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
    TUnmountTableOptions()
        : Force(false)
    { }

    bool Force;
};

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
{ };

struct TTransactionStartOptions
{
    TTransactionStartOptions()
        : Type(NTransactionClient::ETransactionType::Master)
        , AutoAbort(true)
        , Ping(true)
        , PingAncestors(false)
        , Attributes(nullptr)
    { }

    NTransactionClient::ETransactionType Type;
    TNullable<TDuration> Timeout;
    NTransactionClient::TTransactionId ParentId;
    bool AutoAbort;
    bool Ping;
    bool PingAncestors;
    NYTree::IAttributeDictionary* Attributes;
};

struct TLookupRowsOptions
{
    TLookupRowsOptions()
        : Timestamp(NTransactionClient::LastCommittedTimestamp)
    { }

    TNullable<std::vector<Stroka>> ColumnNames;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp;
};

struct TSelectRowsOptions
{
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp;
};

struct TGetNodeOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TGetNodeOptions()
        : Options(nullptr)
    { }

    NYTree::IAttributeDictionary* Options;
    NYTree::TAttributeFilter AttributeFilter;
    TNullable<i64> MaxSize;
};

struct TSetNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TRemoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TRemoveNodeOptions()
        : Recursive(true)
        , Force(false)
    { }

    bool Recursive;
    bool Force;
};

struct TListNodesOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::vector<Stroka> Attributes;
    TNullable<i64> MaxSize;
};

struct TCreateNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TCreateNodeOptions()
        : Attributes(nullptr)
        , Recursive(false)
        , IgnoreExisting(false)
    { }

    NYTree::IAttributeDictionary* Attributes;
    bool Recursive;
    bool IgnoreExisting;
};

struct TLockNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TLockNodeOptions()
        : Waitable(false)
    { }

    bool Waitable;
};

struct TCopyNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TCopyNodeOptions()
        : PreserveAccount(false)
    { }

    bool PreserveAccount;
};

struct TMoveNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TLinkNodeOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TLinkNodeOptions()
        : Attributes(nullptr)
        , Recursive(false)
        , IgnoreExisting(false)
    { }

    NYTree::IAttributeDictionary* Attributes;
    bool Recursive;
    bool IgnoreExisting;
};

struct TNodeExistsOptions
    : public TTransactionalOptions
{ };

struct TCreateObjectOptions
    : public TTransactionalOptions
    , public TMutatingOptions
{
    TCreateObjectOptions()
        : Attributes(nullptr)
    { }

    NYTree::IAttributeDictionary* Attributes;
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
    TFileWriterOptions()
        : Append(true)
    { }

    bool Append;
};

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
        const TTransactionStartOptions& options) = 0;


    // Tables
    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TKey key,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<TErrorOr<IRowsetPtr>> LookupRows(
        const NYPath::TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TAsyncError SelectRows(
        const Stroka& query,
        NVersionedTableClient::ISchemafulWriterPtr writer,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual TFuture<TErrorOr<IRowsetPtr>> SelectRows(
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


    // TODO(babenko): scheduler commands

};

DEFINE_REFCOUNTED_TYPE(IClientBase)

///////////////////////////////////////////////////////////////////////////////

//! A central entry point for all interactions with the YT cluster.
/*!
 *  In contrast to IConnection, each IClient represents an authenticated entity.
 *  The needed username is passed to #CreateClient.
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

    // TODO(babenko): CheckPermission

};

DEFINE_REFCOUNTED_TYPE(IClient)

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options = TClientOptions());

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

