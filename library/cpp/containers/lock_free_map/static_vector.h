#pragma once

#include <util/system/yassert.h>

#include <array>
#include <cstddef>
#include <new>

namespace NLockFreeMap::NPrivate {

    template <class TValue, std::size_t Capacity>
    class TStaticVector {
    public:
        using iterator = TValue*;
        using const_iterator = const TValue*;

        TStaticVector() = default;

        ~TStaticVector() {
            for (auto&& value : *this) {
                std::destroy_at(std::addressof(value));
            }
        }

        const TValue& operator[](std::size_t index) const {
            Y_ABORT_UNLESS(index < Size_);
            return *std::launder(reinterpret_cast<const TValue*>(&Data_[index]));
        }

        TValue& operator[](std::size_t index) {
            Y_ABORT_UNLESS(index < Size_);
            return *std::launder(reinterpret_cast<TValue*>(&Data_[index]));
        }

        template <class... TArgs>
        void EmplaceBack(TArgs&&... args) noexcept(std::is_nothrow_constructible_v<TValue, TArgs...>) {
            Y_ABORT_UNLESS(Size_ < Capacity);
            ::new (&Data_[Size_++]) TValue(std::forward<TArgs>(args)...);
        }

        std::size_t size() const noexcept {
            return Size_;
        }

        iterator begin() noexcept {
            return std::launder(reinterpret_cast<TValue*>(Data_.data()));
        }

        iterator end() noexcept {
            return std::launder(reinterpret_cast<TValue*>(Data_.data() + Size_));
        }

        const_iterator begin() const noexcept {
            return std::launder(reinterpret_cast<const TValue*>(Data_.data()));
        }

        const_iterator end() const noexcept {
            return std::launder(reinterpret_cast<const TValue*>(Data_.data() + Size_));
        }

    private:
        std::size_t Size_;
        std::array<std::aligned_storage_t<sizeof(TValue), alignof(TValue)>, Capacity> Data_;
    };

} // namespace NLockFreeMap::NPrivate
