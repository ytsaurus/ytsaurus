#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/api/table_client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

// BEWARE: Not thread-safe.
struct ISequoiaClient
    : public TRefCounted
{
public:
    virtual TFuture<NApi::IUnversionedRowsetPtr> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        std::optional<NTransactionClient::TTimestamp> timestamp = {}) = 0;

    template <class TRecordKey>
    TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> LookupRows(
        const std::vector<TRecordKey>& keys,
        const NTableClient::TColumnFilter& columnFilter = {},
        std::optional<NTransactionClient::TTimestamp> timestamp = {});

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const std::vector<TString>& whereConjuncts,
        std::optional<i64> limit,
        std::optional<NTransactionClient::TTimestamp> timestamp) = 0;

    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(
        const std::vector<TString>& whereConjunts,
        std::optional<i64> limit = {},
        std::optional<NTransactionClient::TTimestamp> timestamp = {});

    virtual TFuture<ISequoiaTransactionPtr> StartTransaction(
        const NApi::TTransactionStartOptions& options = {}) = 0;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;

    virtual NLogging::TLogger GetLogger() const = 0;
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
