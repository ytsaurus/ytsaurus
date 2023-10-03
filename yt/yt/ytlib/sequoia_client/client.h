#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/table_client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaClient
    : public TRefCounted
{
public:
    virtual TFuture<NApi::IUnversionedRowsetPtr> LookupRows(
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
        const std::vector<TString>& whereConjuncts,
        std::optional<i64> limit,
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp) = 0;

    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        const std::vector<TString>& whereConjunts,
        std::optional<i64> limit = {},
        NTransactionClient::TTimestamp timestamp = NTransactionClient::SyncLastCommittedTimestamp);

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        const NApi::TTransactionStartOptions& options = {}) = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;
    virtual const NApi::NNative::IClientPtr& GetNativeClient() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaClient)

////////////////////////////////////////////////////////////////////////////////

ISequoiaClientPtr CreateSequoiaClient(
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define CLIENT_INL_H_
#include "client-inl.h"
#undef CLIENT_INL_H_
