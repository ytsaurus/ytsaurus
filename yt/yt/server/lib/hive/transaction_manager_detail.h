#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

template <class TTransaction>
class TTransactionManagerBase
    : public virtual NLogging::TLoggerOwner
{
public:
    void RegisterTransactionActionHandlers(
        const TTransactionPrepareActionHandlerDescriptor<TTransaction>& prepareActionDescriptor,
        const TTransactionCommitActionHandlerDescriptor<TTransaction>& commitActionDescriptor,
        const TTransactionAbortActionHandlerDescriptor<TTransaction>& abortActionDescriptor);

protected:
    THashMap<TString, TTransactionPrepareActionHandler<TTransaction>> PrepareActionHandlerMap_;
    THashMap<TString, TTransactionCommitActionHandler<TTransaction>> CommitActionHandlerMap_;
    THashMap<TString, TTransactionAbortActionHandler<TTransaction>> AbortActionHandlerMap_;


    void RunPrepareTransactionActions(TTransaction* transaction, bool persistent);
    void RunCommitTransactionActions(TTransaction* transaction);
    void RunAbortTransactionActions(TTransaction* transaction);

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

} // namespace NYT::NHiveServer

#define TRANSACTION_MANAGER_DETAIL_INL_H_
#include "transaction_manager_detail-inl.h"
#undef TRANSACTION_MANAGER_DETAIL_INL_H_
