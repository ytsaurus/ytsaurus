#pragma once

#include "public.h"

#include "transaction_action.h"

#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

//! Returns |true| if the current thread is executing a transaction action.
bool IsInTransactionAction();

class TTransactionActionGuard
    : private TNonCopyable
{
public:
    TTransactionActionGuard();
    ~TTransactionActionGuard();
};

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction, class TSaveContext, class TLoadContext>
class TTransactionManagerBase
    : public virtual NLogging::TLoggerOwner
    , public ITransactionActionStateFactory<TSaveContext, TLoadContext>
{
protected:
    using TTransactionActionDescriptor = TTypeErasedTransactionActionDescriptor<
        TTransaction,
        TSaveContext,
        TLoadContext
    >;
    THashMap<std::string, TTransactionActionDescriptor, THash<TStringBuf>, TEqualTo<>> ActionDescriptorMap_;

    void RegisterTransactionActionHandlers(
        TTypeErasedTransactionActionDescriptor<TTransaction, TSaveContext, TLoadContext> descriptor);
    void RunPrepareTransactionActions(
        TTransaction* transaction,
        const TTransactionPrepareOptions& options);
    void RunCommitTransactionActions(TTransaction* transaction, const TTransactionCommitOptions& options);
    // COMPAT(kvk1920): drop #requireLegacyBehavior after both Chaos and tablet
    // reigns |FixTransactionActionAbort| are removed.
    void RunAbortTransactionActions(TTransaction* transaction, const TTransactionAbortOptions& options, bool requireLegacyBehavior);
    void RunSerializeTransactionActions(TTransaction* transaction);

private:
    using ITransactionActionState = ITransactionActionState<TSaveContext, TLoadContext>;

    ITransactionActionState* GetOrCreateTransactionActionState(
        typename TTransaction::TAction* action,
        const TTransactionActionDescriptor& descriptor);

    std::unique_ptr<ITransactionActionState> CreateTransactionActionState(TStringBuf type) override;
};

////////////////////////////////////////////////////////////////////////////////

//! Maintains a set of transaction ids of bounded capacity.
//! Expires old ids in FIFO order.
class TTransactionIdPool
{
public:
    explicit TTransactionIdPool(int maxSize);
    void Register(TTransactionId id);
    bool IsRegistered(TTransactionId id) const;

private:
    const int MaxSize_;
    THashSet<TTransactionId> IdSet_;
    TRingQueue<TTransactionId> IdQueue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor

#define TRANSACTION_MANAGER_DETAIL_INL_H_
#include "transaction_manager_detail-inl.h"
#undef TRANSACTION_MANAGER_DETAIL_INL_H_
