#include "transaction_context.h"

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

void TTransactionContext::MergeContext(TTransactionContext&& freshContext)
{
    if (freshContext.Items.empty()) {
        return;
    }
    if (Items.empty()) {
        Items = std::move(freshContext.Items);
        return;
    }
    THashMap<TString, TString> mergedItems;
    mergedItems.reserve(Items.size() + freshContext.Items.size());
    for (auto& [key, value] : freshContext.Items) {
        mergedItems.insert({std::move(key), std::move(value)});
    }
    for (auto& [key, value] : Items) {
        mergedItems.insert({std::move(key), std::move(value)});
    }
    Items = std::move(mergedItems);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    NYT::TStringBuilderBase* builder,
    const TTransactionContext& transactionContext,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("{Items: %v}", transactionContext.Items);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
