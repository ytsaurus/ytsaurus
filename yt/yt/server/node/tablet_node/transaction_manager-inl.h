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
    auto needsExternalization = std::move(descriptor.NeedsExternalization);

    TTypeErasedTransactionActionDescriptor::TBase baseTypeErasedDescriptor(
        std::move(
            static_cast<TTypedTransactionActionDescriptor<TProto, TState>::TBase&>(descriptor)));

    TTypeErasedTransactionActionDescriptor typeErasedDescriptor(std::move(baseTypeErasedDescriptor));

    if (needsExternalization) {
        typeErasedDescriptor.NeedsExternalization = BIND(
            [handler = std::move(needsExternalization)] (
                TTransaction* transaction,
                TStringBuf value,
                TTabletId tabletId)
            {
                TProto typedValue;
                DeserializeProto(&typedValue, TRef::FromStringBuf(value));
                return handler(transaction, &typedValue, tabletId);
            });
    }

    RegisterTransactionActionHandlers(std::move(typeErasedDescriptor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
