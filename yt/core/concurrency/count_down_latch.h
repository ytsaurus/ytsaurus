#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A synchronization aid that allows one or more threads to wait until
// a set of operations being performed in other threads completes.
/*!
 *  See https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/CountDownLatch.html
 */
class TCountDownLatch
{
public:
    explicit TCountDownLatch(size_t count);
    ~TCountDownLatch();

    void CountDown();

    void Wait() const;

    bool TryWait() const;

    size_t GetCount() const;

private:
    std::atomic<size_t> Count_;

#ifndef _linux_
    TCondVar ConditionVariable_;
    TMutex Mutex_;
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
