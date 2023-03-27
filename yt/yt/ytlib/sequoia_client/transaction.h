#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/core/misc/shared_range.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaTransaction
    : public TRefCounted
{
public:
    virtual TFuture<void> Start(const NApi::TTransactionStartOptions& options) = 0;

    virtual TFuture<void> Commit(const NApi::TTransactionCommitOptions& options) = 0;

    virtual TFuture<NApi::IUnversionedRowsetPtr> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter,
        std::optional<NTransactionClient::TTimestamp> timestamp) = 0;

    template <class TRecordKey>
    TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> LookupRows(
        const std::vector<TRecordKey>& keys,
        const NTableClient::TColumnFilter& columnFilter = {},
        std::optional<NTransactionClient::TTimestamp> timestamp = {});

    virtual void DatalessLockRow(
        NObjectClient::TCellTag masterCellTag,
        ESequoiaTable table,
        NTableClient::TLegacyKey key,
        NTableClient::ELockType lockType) = 0;

    template <class TRecordKey>
    void DatalessLockRow(
        NObjectClient::TCellTag masterCellTag,
        const TRecordKey& key,
        NTableClient::ELockType lockType);

    virtual void LockRow(
        ESequoiaTable table,
        NTableClient::TLegacyKey key,
        NTableClient::ELockType lockType) = 0;

    template <class TRecordKey>
    void LockRow(
        const TRecordKey& key,
        NTableClient::ELockType lockType);

    virtual void WriteRow(
        ESequoiaTable table,
        NTableClient::TUnversionedRow row) = 0;

    template <class TRecord>
    void WriteRow(const TRecord& record);

    virtual void DeleteRow(
        ESequoiaTable table,
        NTableClient::TUnversionedRow row) = 0;

    template <class TRecordKey>
    void DeleteRow(const TRecordKey& key);

    virtual void AddTransactionAction(
        NObjectClient::TCellTag masterCellTag,
        NTransactionClient::TTransactionActionData data) = 0;

    virtual NObjectClient::TObjectId GenerateObjectId(
        NObjectClient::EObjectType objectType,
        NObjectClient::TCellTag cellTag,
        bool sequoia = true) = 0;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;
    virtual const NApi::NNative::IClientPtr& GetClient() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

ISequoiaTransactionPtr CreateSequoiaTransaction(
    NApi::NNative::IClientPtr client,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define TRANSACTION_INL_H_
#include "transaction-inl.h"
#undef TRANSACTION_INL_H_
