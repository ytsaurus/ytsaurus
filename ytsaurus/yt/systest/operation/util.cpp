#include <yt/systest/operation/util.h>

namespace NYT::NTest {

std::vector<int> CollectInputColumns(TRange<const IOperation*> operations)
{
    std::vector<int> result;
    for (const auto* operation : operations) {
        std::copy(operation->InputColumns().begin(), operation->InputColumns().end(), std::back_inserter(result));
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

std::vector<TNode> PopulateOperationInput(
    TRange<int> allInputColumns,
    TRange<int> operationInputColumns,
    TRange<TNode> input)
{
    std::vector<TNode> operationInput;
    for (int index : operationInputColumns) {
        int position = std::lower_bound(allInputColumns.begin(), allInputColumns.end(), index)
            - allInputColumns.begin();
        operationInput.push_back(input[position]);
    }

    return operationInput;
}

}  // namespace NYT::NTest
