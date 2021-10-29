#pragma once

#include "yt/yt/library/memory/ref_counted.h"
#include <atomic>

#include <yt/yt/library/memory/intrusive_ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! TAtomicSignalPtr is special kind of reference, that is safe to use from signal handler.
template <class T>
class TAtomicSignalPtr
{
public:
    constexpr TAtomicSignalPtr() noexcept 
    { }

    TAtomicSignalPtr(const TAtomicSignalPtr& other) = delete;

    TIntrusivePtr<T> GetFromSignal()
    {
        return TIntrusivePtr<T>(T_);
    }

    void StoreFromThread(const TIntrusivePtr<T>& ptr)
    {
        if (T_) {
            auto tmp = T_;

            T_ = nullptr;
            std::atomic_signal_fence(std::memory_order_seq_cst);

            Unref(tmp);
        }

        if (ptr) {
            T_ = ptr.Get();
            Ref(T_);
        }
    }

    bool IsSetFromThread()
    {
        return T_ != nullptr;
    }

    ~TAtomicSignalPtr()
    {
        StoreFromThread(nullptr);
    }

private:
    T* T_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
