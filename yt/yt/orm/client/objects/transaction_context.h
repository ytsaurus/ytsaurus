#pragma once

#include "public.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionContext
{
    THashMap<TString, TString> Items;

    bool operator==(const TTransactionContext& rhs) const = default;

    void MergeContext(TTransactionContext&& freshContext);
};

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTransactionContext& transactionContext,
    TStringBuf spec);

template <class TProtoTransactionContext>
void ToProto(
    TProtoTransactionContext* protoTransactionContext,
    const TTransactionContext& context);

template <class TProtoTransactionContext>
void FromProto(
    TTransactionContext* context,
    const TProtoTransactionContext& protoTransactionContext);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

#define TRANSACTION_CONTEXT_INL_H_
#include "transaction_context-inl.h"
#undef TRANSACTION_CONTEXT_INL_H_
