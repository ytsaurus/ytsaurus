#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/small_vector.h>

#include <core/ytree/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/ypath/public.h>

#include <ytlib/hydra/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const std::vector<NVersionedTableClient::TUnversionedRow>& Rows() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowset)

///////////////////////////////////////////////////////////////////////////////

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

    NVersionedTableClient::TColumnFilter ColumnFilter;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp;
};

struct TSelectRowsOptions
{
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp;
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

struct TGetNodeOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    TGetNodeOptions()
        : Options(nullptr)
    { }

    NYTree::IAttributeDictionary* Options;
    std::vector<Stroka> Attributes;
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
{ };

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic set of functions that can be invoked
//! both standalone and inside transaction.
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
        NVersionedTableClient::ISchemedWriterPtr writer,
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

struct ITransaction
    : public IClientBase
{
    virtual NTransactionClient::ETransactionType GetType() const = 0;
    virtual const NTransactionClient::TTransactionId& GetId() const = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const = 0;

    virtual TAsyncError Commit() = 0;
    virtual TAsyncError Abort() = 0;

    // Transaction-only stuff follows.

    // Tables
    virtual void WriteRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TUnversionedRow row) = 0;

    virtual void WriteRows(
        const NYPath::TYPath& path,
        std::vector<NVersionedTableClient::TUnversionedRow> rows) = 0;
    
    virtual void DeleteRow(
        const NYPath::TYPath& path,
        NVersionedTableClient::TKey key) = 0;

    virtual void DeleteRows(
        const NYPath::TYPath& path,
        std::vector<NVersionedTableClient::TKey> keys) = 0;

};

DEFINE_REFCOUNTED_TYPE(ITransaction)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

