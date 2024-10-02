#ifndef TRANSACTION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_context.h"
// For the sake of sane code completion.
#include "transaction_context.h"
#endif

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoTransactionContext>
void ToProto(
    TProtoTransactionContext* protoTransactionContext,
    const TTransactionContext& context)
{
    for (const auto& [key, value] : context.Items) {
        protoTransactionContext->mutable_items()->insert({key, value});
    }
}

template <class TProtoTransactionContext>
void FromProto(
    TTransactionContext* context,
    const TProtoTransactionContext& protoTransactionContext)
{
    context->Items.reserve(protoTransactionContext.items().size());
    for (const auto& [key, value] : protoTransactionContext.items()) {
        context->Items.insert({key, value});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
