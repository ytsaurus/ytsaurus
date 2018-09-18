#include "async_rw_lock.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TAsyncReaderWriterLock::AcquireReader()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (!HasActiveWriter_ && WriterPromiseQueue_.empty()) {
        ++ActiveReaderCount_;
        return VoidFuture;
    }

    auto promise = NewPromise<void>();
    ReaderPromiseQueue_.push_back(promise);
    return promise;
}

void TAsyncReaderWriterLock::ReleaseReader()
{
    TGuard<TSpinLock> guard(SpinLock_);

    YCHECK(ActiveReaderCount_ > 0);

    --ActiveReaderCount_;
    if (ActiveReaderCount_ == 0 && !WriterPromiseQueue_.empty()) {
        auto promise = WriterPromiseQueue_.front();
        WriterPromiseQueue_.pop();
        HasActiveWriter_ = true;
        guard.Release();
        promise.Set();
    }
}

TFuture<void> TAsyncReaderWriterLock::AcquireWriter()
{
    TGuard<TSpinLock> guard(SpinLock_);

    if (ActiveReaderCount_ == 0 && !HasActiveWriter_) {
        HasActiveWriter_ = true;
        return VoidFuture;
    }

    auto promise = NewPromise<void>();
    WriterPromiseQueue_.push(promise);
    return promise;
}

void TAsyncReaderWriterLock::ReleaseWriter()
{
    TGuard<TSpinLock> guard(SpinLock_);

    YCHECK(HasActiveWriter_);

    HasActiveWriter_ = false;
    if (WriterPromiseQueue_.empty()) {
        // Run all readers.
        if (!ReaderPromiseQueue_.empty()) {
            std::vector<TPromise<void>> readerPromiseQueue;
            readerPromiseQueue.swap(ReaderPromiseQueue_);
            ActiveReaderCount_ += readerPromiseQueue.size();
            guard.Release();
            {
                auto setPromises = [&] () {
                    for (auto& promise : readerPromiseQueue) {
                        promise.Set();
                    }
                };
                if (TryGetCurrentScheduler()) {
                    // Promise subscribers must be synchronous to avoid hanging on some reader.
                    TForbidContextSwitchGuard contextSwitchGuard;
                    setPromises();
                } else {
                    setPromises();
                }
            }
        }
    } else {
        auto promise = WriterPromiseQueue_.front();
        WriterPromiseQueue_.pop();
        HasActiveWriter_ = true;
        guard.Release();
        promise.Set();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
