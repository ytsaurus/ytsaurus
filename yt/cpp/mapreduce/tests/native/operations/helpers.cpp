#include "helpers.h"

#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NYT::NTesting {

using ::TStringBuilder;

void WaitOperationPredicate(
    const IOperationPtr& operation,
    const std::function<bool(const TOperationAttributes&)>& predicate,
    const TString& failMsg)
{
    try {
        WaitForPredicate([&] {
            return predicate(operation->GetAttributes());
        });
    } catch (const TWaitFailedException& exception) {
        ythrow yexception() << "Wait for operation " << operation->GetId() << " failed: "
            << failMsg << ".\n" << exception.what();
    }
}

void WaitOperationHasState(const IOperationPtr& operation, const TString& state)
{
    WaitOperationPredicate(
        operation,
        [&] (const TOperationAttributes& attrs) {
            YT_LOG_DEBUG("Operation %s state is %s",
                GetGuidAsString(operation->GetId()).c_str(),
                ToString(attrs.State).c_str());
            return attrs.State == state;
        },
        "state should become \"" + state + "\"");
}

void WaitOperationIsRunning(const IOperationPtr& operation)
{
    WaitOperationHasState(operation, "running");
}

void WaitOperationHasBriefProgress(const IOperationPtr& operation)
{
    WaitOperationPredicate(
        operation,
        [&] (const TOperationAttributes& attributes) {
            return attributes.BriefProgress.Defined();
        },
        "brief progress should have become available");
}

TString GetOperationState(const IClientPtr& client, const TOperationId& operationId)
{
    const auto& state = client->GetOperation(operationId).State;
    UNIT_ASSERT(state.Defined());
    return *state;
}

void EmulateOperationArchivation(IClientPtr& client, const TOperationId& operationId)
{
    auto idStr = GetGuidAsString(operationId);
    auto lastTwoDigits = idStr.substr(idStr.size() - 2, 2);
    TString path = ::TStringBuilder() << "//sys/operations/" << lastTwoDigits << "/" << idStr;
    client->Remove(path, TRemoveOptions().Recursive(true));
}

void CreateTableWithFooColumn(IClientPtr client, const TString& path)
{
    auto writer = client->CreateTableWriter<TNode>(path);
    writer->AddRow(TNode()("foo", "baz"));
    writer->AddRow(TNode()("foo", "bar"));
    writer->Finish();
}

} // namespace NYT::NTesting
