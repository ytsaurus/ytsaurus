#include "task_runner_invoker_factory.h"

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYql::NDqs {
namespace {

class TSerializedTaskRunnerInvoker: public ITaskRunnerInvoker {
public:
    explicit TSerializedTaskRunnerInvoker(const NYT::IInvokerPtr& invoker)
        : Invoker(NYT::NConcurrency::CreateSerializedInvoker(invoker))
    { }

    void Invoke(const std::function<void(void)>& callback) override {
        Invoker->Invoke(BIND(callback));
    }

private:
    const NYT::IInvokerPtr Invoker;
};

class TConcurrentInvokerFactory: public ITaskRunnerInvokerFactory {
public:
    explicit TConcurrentInvokerFactory(int capacity)
        : ThreadPool(NYT::NConcurrency::CreateThreadPool(capacity, "WorkerActor"))
    { }

    ITaskRunnerInvoker::TPtr Create() override {
        return new TSerializedTaskRunnerInvoker(ThreadPool->GetInvoker());
    }

private:
    const NYT::NConcurrency::IThreadPoolPtr ThreadPool;
};

} // anonymous namespace

ITaskRunnerInvokerFactory::TPtr CreateConcurrentInvokerFactory(int capacity) {
    return new TConcurrentInvokerFactory(capacity);
}

} // namespace NYql::NDqs
