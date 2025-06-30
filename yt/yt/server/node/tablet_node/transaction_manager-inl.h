#ifndef TRANSACTION_MANAGER_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_manager.h"
// For the sake of sane code completion.
#include "transaction_manager.h"
#endif

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TProto, class TState>
void ITransactionManager::RegisterTransactionActionHandlers(
    TTypedTransactionActionDescriptor<TProto, TState> descriptor)
{
    RegisterTransactionActionHandlers(TTypeErasedTransactionActionDescriptor(std::move(descriptor)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
