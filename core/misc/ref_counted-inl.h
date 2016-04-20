#ifndef REF_COUNTED_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

inline int AtomicallyIncrementIfNonZero(std::atomic<int>& atomicCounter)
{
    // Atomically performs the following:
    // { auto v = *p; if (v != 0) ++(*p); return v; }
    while (true) {
        auto value = atomicCounter.load();

        if (value == 0) {
            return value;
        }

        if (atomicCounter.compare_exchange_strong(value, value + 1)) {
            return value;
        }
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

