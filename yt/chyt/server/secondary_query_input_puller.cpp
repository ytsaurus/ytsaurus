#include "secondary_query_input_puller.h"

#include "subquery_spec.h"
#include "subquery.h"
#include "query_context.h"
#include "host.h"

#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NClickHouseServer {

using namespace NChunkPools;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryReadTaskPuller);

TSecondaryQueryReadTaskPuller::TSecondaryQueryReadTaskPuller(
    TQueryContext* queryContext,
    DB::ReadTaskCallback nextTaskCallback)
    : NextTaskCallback_(std::move(nextTaskCallback))
    , QueryContext_(queryContext)
    , Invoker_(QueryContext_->Host->GetClickHouseFetcherInvoker())
{ }

void TSecondaryQueryReadTaskPuller::RegisterOperand(int operandIndex, std::vector<TSecondaryQueryReadDescriptors>&& initialTasks)
{
    auto guard = Guard(BufferLock_);
    for (int index = std::ssize(Buffer_); index <= operandIndex; ++index) {
        Buffer_.emplace_back();
    }
    OperandCount_ = std::ssize(Buffer_);

    if (initialTasks.empty()) {
        return;
    }

    for (auto& task : initialTasks) {
        Buffer_[operandIndex].push(MakeFuture(std::move(task)));
    }
}

TFuture<TSecondaryQueryReadDescriptors> TSecondaryQueryReadTaskPuller::PullTask(int operandIndex)
{
    auto guard = Guard(BufferLock_);

    if (Buffer_[operandIndex].empty()) {
        std::vector<TPromise<TSecondaryQueryReadDescriptors>> taskPromises;
        taskPromises.reserve(OperandCount_);
        for (int index = 0; index < OperandCount_; ++index) {
            taskPromises.push_back(NewPromise<TSecondaryQueryReadDescriptors>());
            Buffer_[index].push(taskPromises.back().ToFuture());
        }
        YT_UNUSED_FUTURE(BIND(&TSecondaryQueryReadTaskPuller::DoPullAsync, MakeStrong(this), Passed(std::move(taskPromises)))
            .AsyncVia(Invoker_)
            .Run());
    }

    auto taskFuture = std::move(Buffer_[operandIndex].front());
    Buffer_[operandIndex].pop();

    return taskFuture;
}

void TSecondaryQueryReadTaskPuller::DoPullAsync(std::vector<TPromise<TSecondaryQueryReadDescriptors>> taskPromises)
{
    YT_ASSERT_INVOKER_AFFINITY(Invoker_);

    using namespace NStatisticPath;
    auto timeGuard = QueryContext_->CreateStatisticsTimerGuard("/secondary_query_read_task_puller/do_pull_task"_SP);

    std::string protoReadTaskString;

    bool finished = Finished_.load(std::memory_order::acquire);

    if (!finished) {
        protoReadTaskString = NextTaskCallback_();
    }

    if (protoReadTaskString.empty()) {
        Finished_.store(true, std::memory_order::release);
        static const TSecondaryQueryReadDescriptors emptyReadDescriptors;
        for (const auto& promise : taskPromises) {
            promise.Set(emptyReadDescriptors);
        }
        return;
    }

    NProto::TSecondaryQueryReadTask protoReadTask;
    Y_PROTOBUF_SUPPRESS_NODISCARD protoReadTask.ParseFromString(protoReadTaskString);
    auto readTask = NYT::FromProto<TSecondaryQueryReadTask>(protoReadTask);
    YT_VERIFY(readTask.OperandInputs.size() == taskPromises.size());

    for (const auto& [promise, task] : Zip(taskPromises, readTask.OperandInputs)) {
        promise.Set(task);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSecondaryQueryReadTaskIterator::TSecondaryQueryReadTaskIterator(
    int operandCount,
    const TRange<TSubquery>& subqueries,
    const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap)
{
    EncodedReadTasks_.reserve(subqueries.size());
    for (const auto& subquery : subqueries) {
        TSecondaryQueryReadTask readTask;
        for (int operandIndex = 0; operandIndex < operandCount; ++operandIndex) {
            auto& operandReadTask = readTask.OperandInputs.emplace_back();
            FillDataSliceDescriptors(operandReadTask, miscExtMap, subquery.StripeList->Stripes()[operandIndex]);
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
        return {};
    }
    return EncodedReadTasks_[currentIndex];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
