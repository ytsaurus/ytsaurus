#pragma once

#include "public.h"

#include <util/system/yield.h>

#include <atomic>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Single-writer multiple-readers spin lock.
/*!
 *  Reader-side calls are pretty cheap.
 *  If no writers are present then readers never spin.
 *  The lock is unfair.
 */
class TReaderWriterSpinLock
{
public:
    TReaderWriterSpinLock()
        : Value_(0)
    { }

    void AcquireReader()
    {
        for (int counter = 0; ;++counter) {
            if (TryAcquireReader())
                break;
            if (counter > YieldThreshold) {
                SchedYield();
            }
        }
    }

    void ReleaseReader()
    {
        ui32 prevValue = Value_.fetch_sub(ReaderDelta, std::memory_order_release);
        YASSERT((prevValue & ~WriterMask) != 0);
    }

    void AcquireWriter()
    {
        for (int counter = 0; ;++counter) {
            if (TryAcquireWriter())
                break;
            if (counter > YieldThreshold) {
                SchedYield();
            }
        }
    }

    void ReleaseWriter()
    {
        ui32 prevValue = Value_.fetch_and(~WriterMask, std::memory_order_release);
        YASSERT(prevValue & WriterMask);
    }

    void UpgradeReader()
    {
        for (int counter = 0; ;++counter) {
            if (TryUpgradeReader())
                break;
            if (counter > YieldThreshold) {
                SchedYield();
            }
        }
    }

private:
    std::atomic<ui32> Value_;

    static const ui32 WriterMask = 1;
    static const ui32 ReaderDelta = 2;

    static const int YieldThreshold = 1000;


    bool TryAcquireReader()
    {
        ui32 oldValue = Value_.fetch_add(ReaderDelta, std::memory_order_acquire);
        if (oldValue & WriterMask) {
            Value_.fetch_sub(ReaderDelta, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    bool TryAcquireWriter()
    {
        ui32 expected = 0;
        if (!Value_.compare_exchange_weak(expected, WriterMask, std::memory_order_acquire)) {
            return false;
        }
        return true;
    }

    bool TryUpgradeReader()
    {
        ui32 expected = ReaderDelta;
        if (!Value_.compare_exchange_weak(expected, WriterMask, std::memory_order_acquire)) {
            return false;
        }
        return true;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TReaderGuard;
class TWriterGuard;

class TWriterGuard
{
public:
    TWriterGuard()
        : SpinLock_(nullptr)
    { }

    explicit TWriterGuard(TReaderWriterSpinLock& spinLock)
        : SpinLock_(&spinLock)
    {
        SpinLock_->AcquireWriter();
    }

    TWriterGuard(TWriterGuard&& other)
        : SpinLock_(other.SpinLock_)
    {
        other.SpinLock_ = nullptr;
    }

    TWriterGuard(const TWriterGuard& other) = delete;

    ~TWriterGuard()
    {
        if (SpinLock_) {
            SpinLock_->ReleaseWriter();
        }
    }

    void Release()
    {
        if (SpinLock_) {
            SpinLock_->ReleaseWriter();
            SpinLock_ = nullptr;
        }
    }

private:
    friend class TReaderGuard;

    TReaderWriterSpinLock* SpinLock_;


    static TWriterGuard MakeAcquired(TReaderWriterSpinLock& spinLock)
    {
        TWriterGuard guard;
        guard.SpinLock_ = &spinLock;
        return guard;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TReaderGuard
{
public:
    TReaderGuard()
        : SpinLock_(nullptr)
    { }

    explicit TReaderGuard(TReaderWriterSpinLock& spinLock)
        : SpinLock_(&spinLock)
    {
        SpinLock_->AcquireReader();
    }

    TReaderGuard(TReaderGuard&& other)
        : SpinLock_(other.SpinLock_)
    {
        other.SpinLock_ = nullptr;
    }

    TReaderGuard(const TReaderGuard& other) = delete;

    ~TReaderGuard()
    {
        if (SpinLock_) {
            SpinLock_->ReleaseReader();
        }
    }

    void Release()
    {
        if (SpinLock_) {
            SpinLock_->ReleaseReader();
            SpinLock_ = nullptr;
        }
    }

    TWriterGuard Upgrade()
    {
        auto* spinLock = SpinLock_;
        if (!spinLock) {
            return TWriterGuard();
        }
        spinLock->UpgradeReader();
        SpinLock_ = nullptr;
        return TWriterGuard::MakeAcquired(*spinLock);
    }

private:
    TReaderWriterSpinLock* SpinLock_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
