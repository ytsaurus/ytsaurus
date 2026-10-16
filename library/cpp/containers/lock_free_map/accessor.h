#pragma once

namespace NLockFreeMap::NPrivate {

    template <class TValue, class TGuard>
    class TAccessor {
    public:
        TAccessor() = default;
        TAccessor(TValue* value, TGuard guard) noexcept;

        TAccessor(TAccessor&&) = default;
        TAccessor& operator=(TAccessor&&) = default;

        TValue* operator->() const noexcept;
        TValue& operator*() const noexcept;
        TValue* Get() const noexcept;

        explicit operator bool() const noexcept;

    private:
        TValue* const Value_{};
        TGuard Guard_{};
    };

    template <class TValue, class TGuard>
    bool operator==(const TAccessor<TValue, TGuard>& left, const TAccessor<TValue, TGuard>& right) noexcept;

} // namespace NLockFreeMap::NPrivate

#define INCLUDE_ACCESSOR_INL_H
#include "accessor-inl.h"
#undef INCLUDE_ACCESSOR_INL_H
