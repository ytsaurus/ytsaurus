#pragma once

#include "public.h"
#include "helpers.h"
#include "table_descriptor.h"
#include "transaction_options.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/client/api/table_client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaClient
    : public TRefCounted
{
    virtual TFuture<NApi::TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;

    template <class TRecordKey>
    TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> LookupRows(
        const std::vector<TRecordKey>& keys,
        const NTableClient::TColumnFilter& columnFilter = {},
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;
    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        const TSequoiaTablePathDescriptor& tableDescriptor,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;

    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);
    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        NObjectClient::TCellTag masterCellTag,
        const TSelectRowsQuery& query,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);

    virtual TFuture<void> TrimTable(
        const TSequoiaTablePathDescriptor& descriptor,
        int tabletIndex,
        i64 trimmedRowCount) = 0;

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        ESequoiaTransactionType type,
        const NApi::TTransactionStartOptions& options = {},
        const TSequoiaTransactionOptions& sequoiaTransactionOptions = {}) = 0;

    virtual NRpc::TAuthenticationIdentity GetAuthenticationIdentity() const = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaClient)

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NApi::NNative::IClientPtr localAuthenticatedClient,
    TFuture<NApi::NNative::IClientPtr> groundClientFuture);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define CLIENT_INL_H_
#include "client-inl.h"
#undef CLIENT_INL_H_
