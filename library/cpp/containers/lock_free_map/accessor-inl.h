#ifndef INCLUDE_ACCESSOR_INL_H
    #error "include accessor.h instead"
    #include "accessor.h"
#endif

#include <utility>

namespace NLockFreeMap::NPrivate {

    template <class TValue, class TGuard>
    TAccessor<TValue, TGuard>::TAccessor(TValue* value, TGuard guard) noexcept
        : Value_(value)
        , Guard_(std::move(guard))
    {}

    template <class TValue, class TGuard>
    TValue* TAccessor<TValue, TGuard>::operator->() const noexcept {
        return Value_;
    }

    template <class TValue, class TGuard>
    TValue& TAccessor<TValue, TGuard>::operator*() const noexcept {
        return *Value_;
    }

    template <class TValue, class TGuard>
    TValue* TAccessor<TValue, TGuard>::Get() const noexcept {
        return Value_;
    }

    template <class TValue, class TGuard>
    TAccessor<TValue, TGuard>::operator bool() const noexcept {
        return Value_;
    }

    template <class TValue, class TGuard>
    bool operator==(const TAccessor<TValue, TGuard>& left, const TAccessor<TValue, TGuard>& right) noexcept {
        return left.Get() == right.Get();
    }

} // namespace NLockFreeMap::NPrivate
