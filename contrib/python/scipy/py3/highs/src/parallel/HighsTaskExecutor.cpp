#include "parallel/HighsTaskExecutor.h"

using namespace highs;

#ifdef _WIN32
static thread_local HighsSplitDeque* threadLocalWorkerDequePtr{nullptr};
HighsSplitDeque*& HighsTaskExecutor::threadLocalWorkerDeque() {
  return threadLocalWorkerDequePtr;
}

static thread_local HighsTaskExecutor::ExecutorHandle globalExecutorHandle{};

HighsTaskExecutor::ExecutorHandle&
HighsTaskExecutor::threadLocalExecutorHandle() {
  return globalExecutorHandle;
}
#else
thread_local HighsSplitDeque* HighsTaskExecutor::threadLocalWorkerDequePtr{
    nullptr};
thread_local HighsTaskExecutor::ExecutorHandle
    HighsTaskExecutor::globalExecutorHandle{};
#endif

void HighsTaskExecutor::ExecutorHandle::dispose() {
  if (ptr == nullptr) return;
  if (isMain) {
    ptr->stopWorkerThreads(false);
  }

  // check to see if we are the last handle and if so, delete the executor
  if (--ptr->referenceCount == 0) {
    cache_aligned::Deleter<HighsTaskExecutor>()(ptr);
  }

  ptr = nullptr;
}
