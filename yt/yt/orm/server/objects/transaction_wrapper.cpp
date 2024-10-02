#include "transaction_wrapper.h"

#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/orm/server/master/bootstrap.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NConcurrency;
using namespace NClient;

////////////////////////////////////////////////////////////////////////////////

TTransactionWrapper::TTransactionWrapper(
    const TTransactionManagerPtr& transactionManager,
    TTransactionId id,
    std::optional<TDuration> requestTimeout,
    std::optional<bool> allowFullScan,
    bool mustOwn,
    bool flushPerformanceStatistics)
{
    if (id) {
        Owned_ = false;

        Transaction_ = transactionManager->GetTransactionIfConnectedOrThrow(id);
        LockGuard_ = Transaction_->AcquireLock();
        if (auto state = Transaction_->GetState(); state != ETransactionState::Active) {
            Transaction_ = nullptr;
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidTransactionState,
                "Transaction %v is in %Qlv state", id, state);
        }
        if (flushPerformanceStatistics) {
            Transaction_->FlushPerformanceStatistics();
        }
    } else {
        if (!mustOwn) {
            THROW_ERROR_EXCEPTION(EErrorCode::InvalidTransactionId,
                "Null transaction id is not allowed");
        }
        Owned_ = true;
        Transaction_ = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();
    }
    if (allowFullScan.has_value()) {
        Transaction_->AllowFullScan(*allowFullScan);
    }
    Transaction_->UpdateRequestTimeout(std::move(requestTimeout));
}

TTransactionWrapper::~TTransactionWrapper()
{
    if (std::uncaught_exception() && Transaction_) {
        YT_UNUSED_FUTURE(Transaction_->Abort());
    }
}

const TTransactionPtr& TTransactionWrapper::Unwrap() const
{
    return Transaction_;
}

std::optional<TTimestamp> TTransactionWrapper::CommitIfOwned() const
{
    if (!Owned_) {
        return {};
    }

    const auto result = WaitFor(Transaction_->Commit())
        .ValueOrThrow();
    return result.CommitTimestamp;
}

////////////////////////////////////////////////////////////////////////////////

TReadOnlyTransactionWrapper::TReadOnlyTransactionWrapper(
    const TTransactionManagerPtr& transactionManager,
    TTimestampOrTransactionId timestampOrTransactionId,
    std::optional<TDuration> requestTimeout,
    std::optional<bool> allowFullScan,
    bool flushPerformanceStatistics)
{
    TStartReadOnlyTransactionOptions startOptions{};
    Visit(timestampOrTransactionId,
        [&] (TTimestamp timestamp) {
            startOptions.StartTimestamp = timestamp;
        },
        [&] (TTransactionId id) {
            if (id) {
                // Create a new transaction object to allow client make concurrent read request in one transaction.
                auto transaction = transactionManager->GetTransactionIfConnectedOrThrow(id);
                startOptions.StartTimestamp = transaction->GetStartTimestamp();
                startOptions.ReadingTransactionOptions.AllowFullScan.emplace(transaction->FullScanAllowed());
            }
        });

    Transaction_ = WaitFor(transactionManager->StartReadOnlyTransaction(startOptions))
        .ValueOrThrow();

    if (allowFullScan.has_value()) {
        Transaction_->AllowFullScan(*allowFullScan);
    }

    Transaction_->UpdateRequestTimeout(requestTimeout);
    if (flushPerformanceStatistics) {
        Transaction_->FlushPerformanceStatistics();
    }
}

const TTransactionPtr& TReadOnlyTransactionWrapper::Unwrap() const
{
    return Transaction_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
