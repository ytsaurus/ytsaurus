#pragma once

#include <util/system/spinlock.h>

#include <thread>

namespace NLockFreeMap {

    struct TNoneBackoff {
        void operator()() const noexcept {}
    };

    struct TYieldBackoff {
        void operator()() const noexcept {
            std::this_thread::yield();
        }
    };

    struct TPauseBackoff {
        void operator()() const noexcept {
            SpinLockPause();
        }
    };

    template <std::size_t Max>
    requires(Max > 0)
    class TExponentialBackoff {
    public:
        void operator()() {
            for (std::size_t i = 0; i < Count_; ++i) {
                SpinLockPause();
            }
            Count_ = std::min(Max, Count_ * 2);
        }

    private:
        std::size_t Count_ = 1;
    };

} // namespace NLockFreeMap
