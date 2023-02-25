#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT::NTesting {

void WaitOperationPredicate(
    const IOperationPtr& operation,
    const std::function<bool(const TOperationAttributes&)>& predicate,
    const TString& failMsg = "");

void WaitOperationHasState(const IOperationPtr& operation, const TString& state);

void WaitOperationIsRunning(const IOperationPtr& operation);

void WaitOperationHasBriefProgress(const IOperationPtr& operation);

TString GetOperationState(const IClientPtr& client, const TOperationId& operationId);

void EmulateOperationArchivation(IClientPtr& client, const TOperationId& operationId);

void CreateTableWithFooColumn(IClientPtr client, const TString& path);

} // namespace NYT::NTesting
