#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/server/hydra/composite_automaton.h>

namespace NYT {
namespace NHiveServer {

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

} // namespace NHiveServer
} // namespace NYT

#define TRANSACTION_MANAGER_DETAIL_INL_H_
#include "transaction_manager_detail-inl.h"
#undef TRANSACTION_MANAGER_DETAIL_INL_H_
