#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaTransaction
    : public TRefCounted
{
public:
    virtual TFuture<void> Start(const NApi::NNative::TNativeTransactionStartOptions& options) = 0;

    virtual TFuture<void> Commit(const NApi::TTransactionCommitOptions& options) = 0;

    virtual void DatalessLockRow(
        NObjectClient::TCellTag masterCellTag,
        ESequoiaTable table,
        NTableClient::TLegacyKey key,
        NTableClient::ELockType lockType) = 0;

    template <class TRow>
    void DatalessLockRow(
        NObjectClient::TCellTag masterCellTag,
        const TRow& row,
        NTableClient::ELockType lockType);

    virtual void LockRow(
        ESequoiaTable table,
        NTableClient::TLegacyKey key,
        NTableClient::ELockType lockType) = 0;

    template <class TRow>
    void LockRow(
        const TRow& row,
        NTableClient::ELockType lockType);

    virtual void WriteRow(
        ESequoiaTable table,
        NTableClient::TUnversionedRow row) = 0;

    template <class TRow>
    void WriteRow(const TRow& row);

    virtual void DeleteRow(
        ESequoiaTable table,
        NTableClient::TUnversionedRow row) = 0;

    template <class TRow>
    void DeleteRow(const TRow& row);

    virtual void AddTransactionAction(
        NObjectClient::TCellTag masterCellTag,
        NTransactionClient::TTransactionActionData data) = 0;

    virtual NObjectClient::TObjectId GenerateObjectId(
        NObjectClient::EObjectType objectType,
        NObjectClient::TCellTag cellTag) = 0;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;
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
