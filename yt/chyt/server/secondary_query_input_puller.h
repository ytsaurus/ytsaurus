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

    TFuture<TSecondaryQueryReadDescriptors> PullTask(int operandIndex);

private:
    TQueryContext* QueryContext_;
    IInvokerPtr Invoker_;
    bool Finished_ = false;
    const DB::ReadTaskCallback NextTaskCallback_;

    std::vector<std::queue<TSecondaryQueryReadDescriptors>> Buffer_;

    TSecondaryQueryReadDescriptors DoPullTask(int operandIndex);
    void TryPopulateBuffer();
};

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQueryReadTaskIterator final
{
public:
    TSecondaryQueryReadTaskIterator(
        int operandCount,
        const std::vector<TSubquery>& subqueries,
        const THashMap<NChunkClient::TChunkId, NChunkClient::TRefCountedMiscExtPtr>& miscExtMap);

    std::string NextTask();

private:
    std::atomic<size_t> Index_ = 0;
    std::vector<std::string> EncodedReadTasks_;
};

DEFINE_REFCOUNTED_TYPE(TSecondaryQueryReadTaskIterator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
