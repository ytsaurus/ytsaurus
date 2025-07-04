#pragma once

#include "public.h"
#include "table_descriptor.h"
#include "helpers.h"

#include <yt/yt/ytlib/api/native/transaction.h>

#include <library/cpp/yt/memory/shared_range.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaTransaction
    : public TRefCounted
{
    class TThreadSafeRowBuffer
    {
    public:
        TThreadSafeRowBuffer(
            NThreading::TSpinLock* lock,
            NTableClient::TRowBufferPtr rowBuffer);

        const NTableClient::TRowBufferPtr& Get() const;

    private:
        const TGuard<NThreading::TSpinLock> Guard_;
        const NTableClient::TRowBufferPtr RowBuffer_;
    };

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
        const TSelectRowsQuery& query) = 0;

    template <class TRecord>
    TFuture<std::vector<TRecord>> SelectRows(const TSelectRowsQuery& query);

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
    virtual void WriteRow(
        TSequoiaTablePathDescriptor tableDescriptor,
        NTableClient::TUnversionedRow row,
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive) = 0;

    template <class TRecord>
    void WriteRow(
        const TRecord& record,
        NTableClient::ELockType lockType = NTableClient::ELockType::Exclusive,
        NTableClient::EValueFlags flags = NTableClient::EValueFlags::None);
    template <class TRecord>
    void WriteRow(
        NObjectClient::TCellTag masterCellTag,
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

    virtual TThreadSafeRowBuffer GetGuardedRowBuffer() = 0;
    virtual const ISequoiaClientPtr& GetClient() const = 0;

    virtual NObjectClient::TTransactionId GetId() const = 0;
    virtual NTransactionClient::TTimestamp GetStartTimestamp() const = 0;

    virtual NObjectClient::TCellTagList GetAffectedMasterCellTags() const = 0;

    //! Returns |true| if a given object ID _could_ be generated by this
    //! transaction.
    /*!
     *  Every Sequoia object ID contains tx start timestamp which allows us to
     *  easily determine wether or not a certain object ID could have been
     *  generated by this transaction.
     */
    virtual bool CouldGenerateId(NObjectClient::TObjectId id) const noexcept = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TFuture<ISequoiaTransactionPtr> StartSequoiaTransaction(
    ISequoiaClientPtr client,
    ESequoiaTransactionType type,
    NApi::NNative::IClientPtr localClient,
    NApi::NNative::IClientPtr groundClient,
    const NApi::TTransactionStartOptions& transactionStartOptions,
    const TSequoiaTransactionOptions& sequoiaTransactionOptions);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

#define TRANSACTION_INL_H_
#include "transaction-inl.h"
#undef TRANSACTION_INL_H_
