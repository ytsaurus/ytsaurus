#include "transaction_rotator.h"

#include "transaction.h"
#include "transaction_manager.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NTransactionServer {

using namespace NCellMaster;
using namespace NYTree;

using NHydra::HasHydraContext;

////////////////////////////////////////////////////////////////////////////////

TTransactionRotator::TTransactionRotator(
    TBootstrap* bootstrap,
    TString transactionTitle)
    : Bootstrap_(bootstrap)
    , TransactionTitle_(std::move(transactionTitle))
{
    YT_VERIFY(Bootstrap_);
}

void TTransactionRotator::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    PreviousTransaction_.ResetOnClear();
    Transaction_.ResetOnClear();
}

void TTransactionRotator::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Transaction_);
    Persist(context, PreviousTransaction_);
}

void TTransactionRotator::Rotate()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasHydraContext());

    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    // NB: Transaction commit can lead to OnTransactionFinished(), so we have
    // to ensure that OnTransactionFinished() does not return |true| for
    // this committed transaction.
    auto previousTransaction = std::exchange(PreviousTransaction_, {});
    if (IsObjectAlive(previousTransaction.Get())) {
        transactionManager->CommitMasterTransaction(
            previousTransaction.Get(),
            /*commitOptions*/ {});
    }

    PreviousTransaction_ = std::move(Transaction_);

    Transaction_.Assign(transactionManager->StartTransaction(
        /*parent*/ nullptr,
        /*prerequisiteTransactions*/ {},
        /*replicatedToCellTags*/ {},
        /*timeout*/ std::nullopt,
        /*deadline*/ std::nullopt,
        TransactionTitle_,
        EmptyAttributes(),
        /*isCypressTransaction*/ true));
}

TTransactionId TTransactionRotator::TransactionIdFromPtr(const TTransactionWeakPtr& ptr)
{
    return IsObjectAlive(ptr) ? ptr->GetId() : NullTransactionId;
}

TTransactionId TTransactionRotator::GetTransactionId() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return TransactionIdFromPtr(Transaction_);
}

TTransaction* TTransactionRotator::GetTransaction() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return Transaction_.Get();
}

TTransactionId TTransactionRotator::GetPreviousTransactionId() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return TransactionIdFromPtr(PreviousTransaction_);
}

bool TTransactionRotator::OnTransactionFinished(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (PreviousTransaction_.Get() != transaction &&
        Transaction_.Get() != transaction)
    {
        return false;
    }

    if (Transaction_.Get() == transaction) {
        Transaction_.Reset();
    } else {
        PreviousTransaction_.Reset();
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
