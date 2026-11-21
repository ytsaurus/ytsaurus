#include "async_queue.h"

namespace NYql {

TAsyncQueue::TAsyncQueue(size_t numThreads, const TString& poolName)
    : MtpQueue_(new TSimpleThreadPool(TThreadPoolParams{poolName})) {
    MtpQueue_->Start(numThreads);
}

TAsyncQueue::TAsyncQueue()
    : MtpQueue_(new TFakeThreadPool()) {
    MtpQueue_->Start(1U);
}

TAsyncQueue::TPtr TAsyncQueue::Make(size_t numThreads, const TString& poolName) {
    class TAsyncQueueImpl: public TAsyncQueue {
    public:
        TAsyncQueueImpl(size_t numThreads, const TString& poolName)
            : TAsyncQueue(numThreads, poolName)
        {
        }
    };
    return std::make_shared<TAsyncQueueImpl>(numThreads, poolName);
}

TAsyncQueue::TPtr TAsyncQueue::MakeFakePool() {
    class TAsyncQueueImpl: public TAsyncQueue {
    public:
        TAsyncQueueImpl()
            : TAsyncQueue()
        {
        }
    };
    return std::make_shared<TAsyncQueueImpl>();
}

} // namespace NYql
