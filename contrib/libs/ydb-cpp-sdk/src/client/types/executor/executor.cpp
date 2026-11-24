#include <ydb-cpp-sdk/client/types/executor/executor.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/executor/executor_impl.h>
#include <src/client/impl/internal/thread_pool/pool.h>
#undef INCLUDE_YDB_INTERNAL_H


namespace NYdb::inline V3 {

std::shared_ptr<IExecutor> CreateThreadPoolExecutor(std::size_t threadCount, std::size_t maxQueueSize) {
    return std::make_shared<TThreadPoolExecutor>(CreateThreadPool(threadCount), threadCount, maxQueueSize);
}

}
