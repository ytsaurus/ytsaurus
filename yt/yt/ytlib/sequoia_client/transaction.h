#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/core/misc/shared_range.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaTransaction
    : public TRefCounted
{
    virtual TFuture<void> Commit(const NApi::TTransactionCommitOptions& options = {}) = 0;

    virtual TFuture<NApi::TUnversionedLookupRowsResult> LookupRows(
        ESequoiaTable table,
        TSharedRange<NTableClient::TLegacyKey> keys,
        const NTableClient::TColumnFilter& columnFilter) = 0;

    template <class TRecordKey>
    TFuture<std::vector<std::optional<typename TRecordKey::TRecordDescriptor::TRecord>>> LookupRows(
        const std::vector<TRecordKey>& keys,
        const NTableClient::TColumnFilter& columnFilter = {});

    virtual TFuture<NApi::TSelectRowsResult> SelectRows(
        ESequoiaTable table,
        const TSelectRowsRequest& request) = 0;

    template <class TRecordKey>
    TFuture<std::vector<typename TRecordKey::TRecordDescriptor::TRecord>> SelectRows(
        const TSelectRowsRequest& request);

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
        NTableClient::TUnversionedRow row,
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive) = 0;

    template <class TRecord>
    void WriteRow(
        const TRecord& record,
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive,
        NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);

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
        NObjectClient::TCellTag cellTag = NObjectClient::InvalidCellTag,
        bool sequoia = true) = 0;

    virtual NObjectClient::TCellTag GetRandomSequoiaNodeHostCellTag() const = 0;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;
    virtual const ISequoiaClientPtr& GetClient() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TFuture<ISequoiaTransactionPtr> StartSequoiaTransaction(
    ISequoiaClientPtr client,
    NApi::NNative::IClientPtr nativeRootClient,
    NApi::NNative::IClientPtr groundRootClient,
    const NApi::TTransactionStartOptions& options = {});

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define TRANSACTION_INL_H_
#include "transaction-inl.h"
#undef TRANSACTION_INL_H_
