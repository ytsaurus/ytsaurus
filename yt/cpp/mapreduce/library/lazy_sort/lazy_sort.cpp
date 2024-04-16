#include "lazy_sort.h"

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/algorithm.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static bool IsPrefix(const TVector<TSortColumn>& columns, const TNode::TListType& nodeList) {
    auto mismatch = Mismatch(columns.begin(), columns.end(), nodeList.begin(), nodeList.end(),
        [](const TSortColumn& column, const TNode& node) {
            return column == node.AsString();
        });
    return mismatch.first == columns.end(); // No mismatch happened
};

////////////////////////////////////////////////////////////////////////////////

IOperationPtr LazySort(
    const IClientBasePtr& client,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    TVector<NThreading::TFuture<TNode>> attributesFutures;
    auto batchRequest = client->CreateBatchRequest();
    for (const auto& path : spec.Inputs_) {
        auto options = TGetOptions()
            .AttributeFilter(TAttributeFilter()
                .AddAttribute("sorted")
                .AddAttribute("sorted_by"));
        attributesFutures.push_back(batchRequest->Get(path.Path_ + "/@", options));
    }
    batchRequest->ExecuteBatch();

    bool alreadySorted = AllOf(attributesFutures.begin(), attributesFutures.end(),
        [&](const NThreading::TFuture<TNode>& attributesFuture) {
            const auto& attributes = attributesFuture.GetValue();
            return attributes["sorted"].AsBool() &&
                IsPrefix(spec.SortBy_.Parts_, attributes["sorted_by"].AsList());
        });

    if (alreadySorted) {
        auto mergeSpec = TMergeOperationSpec()
            .Mode(EMergeMode::MM_SORTED)
            .MergeBy(spec.SortBy_)
            .Output(spec.Output_);
        mergeSpec.Inputs_ = spec.Inputs_;
        return client->Merge(mergeSpec, options);
    } else {
        return client->Sort(spec, options);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
