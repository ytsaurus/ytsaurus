#pragma once

#include "public.h"
#include "transaction.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <core/ytree/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

//! Describes settings for a newly created transaction.
struct TTransactionStartOptions
{
    TTransactionStartOptions();

    NTransactionClient::ETransactionType Type;
    TNullable<TDuration> Timeout;
    NTransactionClient::TTransactionId ParentId;
    bool AutoAbort;
    bool Ping;
    bool PingAncestors;
    std::shared_ptr<NYTree::IAttributeDictionary> Attributes; // to make the type copyable
};

///////////////////////////////////////////////////////////////////////////////

struct IRowset
    : public virtual TRefCounted
{
    virtual const std::vector<NVersionedTableClient::TVersionedRow>& Rows() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

struct IClient
    : public virtual TRefCounted
{
    virtual TFuture<TErrorOr<ITransactionPtr>> StartTransaction(
        const TTransactionStartOptions& options) = 0;

    virtual TFuture<TErrorOr<IRowsetPtr>> Lookup(
        const NYPath::TYPath& tablePath,
        NVersionedTableClient::TKey key,
        const TLookupOptions& options = TLookupOptions()) = 0;
};

IClientPtr CreateClient(IConnectionPtr connection);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

