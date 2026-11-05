#include "retryable_transaction.h"

namespace NYT::NFlow {

using namespace NApi;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TRetryableTransaction
    : public IRetryableTransaction
{
public:
#define POSTPONING_METHOD(method, signature, args, range)     \
    void method signature override                            \
    {                                                         \
        if (range.Empty()) {                                  \
            return;                                           \
        }                                                     \
        Apply(BIND([=] (const ITransactionPtr& transaction) { \
            transaction->method args;                         \
        }));                                                  \
    }

    POSTPONING_METHOD(WriteRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TUnversionedRow> rows,
            const NApi::TModifyRowsOptions& options,
            ELockType lockType),
        (path, nameTable, rows, options, lockType),
        rows);

    POSTPONING_METHOD(WriteRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TVersionedRow> rows,
            const NApi::TModifyRowsOptions& options),
        (path, nameTable, rows, options),
        rows);

    POSTPONING_METHOD(DeleteRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TLegacyKey> keys,
            const NApi::TModifyRowsOptions& options),
        (path, nameTable, keys, options),
        keys);

    POSTPONING_METHOD(LockRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TLegacyKey> keys,
            TLockMask lockMask),
        (path, nameTable, keys, lockMask),
        keys);

    POSTPONING_METHOD(LockRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TLegacyKey> keys,
            ELockType lockType),
        (path, nameTable, keys, lockType),
        keys);

    POSTPONING_METHOD(LockRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<TLegacyKey> keys,
            const std::vector<std::string>& locks,
            ELockType lockType),
        (path, nameTable, keys, locks, lockType),
        keys);

    POSTPONING_METHOD(ModifyRows,
        (
            const TYPath& path,
            TNameTablePtr nameTable,
            TSharedRange<NApi::TRowModification> modifications,
            const NApi::TModifyRowsOptions& options),
        (path, nameTable, modifications, options),
        modifications);

#undef POSTPONING_METHOD

    void Apply(TCallback<void(const NApi::ITransactionPtr&)> transactionWriter) override
    {
        TransactionWriters_.push_back(std::move(transactionWriter));
    }

    void DoAttempt(const NApi::ITransactionPtr& transaction) override
    {
        for (const auto& transactionWriter : TransactionWriters_) {
            transactionWriter(transaction);
        }
    }

    bool IsEmpty() override
    {
        return TransactionWriters_.empty();
    }

    void SubscribeOnAttemptResult(TOnAttemptResultCallback callback) override
    {
        OnAttemptResultCallbacks_.push_back(std::move(callback));
    }

    void OnAttemptResult(const TCommitAttemptResult& result) override
    {
        for (const auto& callback : OnAttemptResultCallbacks_) {
            callback(result);
        }
    }

private:
    std::vector<TCallback<void(const NApi::ITransactionPtr&)>> TransactionWriters_;
    std::vector<TOnAttemptResultCallback> OnAttemptResultCallbacks_;
};

////////////////////////////////////////////////////////////////////////////////

IRetryableTransactionPtr CreateRetryableTransaction()
{
    return New<TRetryableTransaction>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
