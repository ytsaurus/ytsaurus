#pragma once

#include "private.h"

#include <yt/yt/client/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <Interpreters/Context.h>
#include <base/types.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryReadTaskPuller final
{
public:
    TSecondaryQueryReadTaskPuller(TQueryContext* queryContext, DB::ReadTaskCallback nextTaskCallback);

    void RegisterOperand(int operandIndex, std::vector<TSecondaryQueryReadDescriptors>&& initialTasks);
    TFuture<TSecondaryQueryReadDescriptors> PullTask(int operandIndex);

private:
    const DB::ReadTaskCallback NextTaskCallback_;
    TQueryContext* QueryContext_;
    IInvokerPtr Invoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, BufferLock_);
    std::vector<std::queue<TFuture<TSecondaryQueryReadDescriptors>>> Buffer_;
    int OperandCount_;

    std::atomic<bool> Finished_ = false;

    void DoPullAsync(std::vector<TPromise<TSecondaryQueryReadDescriptors>> taskPromises);
};

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryReadTaskIterator final
{
public:
    TSecondaryQueryReadTaskIterator(
        int operandCount,
        const TRange<TSubquery>& subqueries,
        const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap);

    std::string NextTask();

private:
    std::atomic<size_t> Index_ = 0;
    std::vector<std::string> EncodedReadTasks_;
};

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryReadTaskIterator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
