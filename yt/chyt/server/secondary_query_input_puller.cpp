#include "secondary_query_input_puller.h"

#include "subquery_spec.h"
#include "subquery.h"
#include "query_context.h"
#include "host.h"

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NClickHouseServer {

using namespace NChunkPools;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryReadTaskPuller);

TSecondaryQueryReadTaskPuller::TSecondaryQueryReadTaskPuller(
    TQueryContext* queryContext,
    DB::ReadTaskCallback nextTaskCallback)
    : QueryContext_(queryContext)
    , Invoker_(NConcurrency::CreateSerializedInvoker(QueryContext_->Host->GetWorkerInvoker()))
    , NextTaskCallback_(std::move(nextTaskCallback))
{ }

TFuture<TSecondaryQueryReadDescriptors> TSecondaryQueryReadTaskPuller::PullTask(int operandIndex)
{
    return BIND(&TSecondaryQueryReadTaskPuller::DoPullTask, MakeStrong(this), operandIndex)
        .AsyncVia(Invoker_)
        .Run();
}

TSecondaryQueryReadDescriptors TSecondaryQueryReadTaskPuller::DoPullTask(int operandIndex)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    using namespace NStatisticPath;
    auto timeGuard = QueryContext_->CreateStatisticsTimerGuard("/secondary_query_read_task_puller/do_pull_task"_SP);

    if (Buffer_.empty() || Buffer_[operandIndex].empty()) {
        TryPopulateBuffer();
    }

    if (Buffer_.empty() || Buffer_[operandIndex].empty()) {
        return {};
    }

    auto readTask = std::move(Buffer_[operandIndex].front());
    Buffer_[operandIndex].pop();

    return readTask;
}

void TSecondaryQueryReadTaskPuller::TryPopulateBuffer()
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    if (Finished_) {
        return;
    }

    auto protoReadTaskString = NextTaskCallback_();
    if (protoReadTaskString.empty()) {
        Finished_ = true;
        return;
    }

    NProto::TSecondaryQueryReadTask protoReadTask;
    Y_PROTOBUF_SUPPRESS_NODISCARD protoReadTask.ParseFromString(protoReadTaskString);
    auto readTask = NYT::FromProto<TSecondaryQueryReadTask>(protoReadTask);

    if (Buffer_.empty()) {
        Buffer_.resize(readTask.OperandInputs.size());
    }

    for (int operandIndex = 0; operandIndex < std::ssize(readTask.OperandInputs); ++operandIndex) {
        Buffer_[operandIndex].push(std::move(readTask.OperandInputs[operandIndex]));
    }
}

////////////////////////////////////////////////////////////////////////////////

TSecondaryQueryReadTaskIterator::TSecondaryQueryReadTaskIterator(
    int operandCount,
    const std::vector<TSubquery>& subqueries,
    const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap)
{
    EncodedReadTasks_.reserve(subqueries.size());
    for (const auto& subquery : subqueries) {
        TSecondaryQueryReadTask readTask;
        for (int operandIndex = 0; operandIndex < operandCount; ++operandIndex) {
            auto& operandReadTask = readTask.OperandInputs.emplace_back();
            FillDataSliceDescriptors(operandReadTask, miscExtMap, subquery.StripeList->Stripes[operandIndex]);
        }

        auto protoReadTask = NYT::ToProto<NProto::TSecondaryQueryReadTask>(readTask);
        auto encodedReadTask = protoReadTask.SerializeAsString();
        EncodedReadTasks_.emplace_back(std::move(encodedReadTask));
    }
}

std::string TSecondaryQueryReadTaskIterator::NextTask()
{
    auto currentIndex = Index_.fetch_add(1);
    if (currentIndex >= EncodedReadTasks_.size()) {
        return "";
    }
    return EncodedReadTasks_[currentIndex];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
