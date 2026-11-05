#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

namespace NHp::NUtil {

    template <class T>
    concept CIsVoid = std::is_void_v<std::remove_const_t<T>>;

    template <class TValue>
    concept CMarkable = CIsVoid<TValue> || alignof(TValue) > 1;

    template <class TValue>
    class TMarkedPtr {
    public:
        using element_type = TValue;

    public:
        TMarkedPtr() noexcept = default;
        TMarkedPtr(std::nullptr_t) noexcept {}

        template <class _TValue>
        requires std::convertible_to<_TValue*, TValue*>
        TMarkedPtr(_TValue* other) noexcept
            : Ptr_(MakeMarkedPtr(other, false))
        {}

        template <class _TValue>
        requires std::convertible_to<_TValue*, TValue*>
        TMarkedPtr(_TValue* other, bool marked) noexcept
            : Ptr_(MakeMarkedPtr(other, marked))
        {}

        template <class _TValue>
        requires std::convertible_to<_TValue*, TValue*>
        TMarkedPtr(const TMarkedPtr<_TValue>& other) noexcept
            : Ptr_(MakeMarkedPtr(other.Get(), other.IsMarked()))
        {}

        template <class _TValue>
        requires std::convertible_to<_TValue*, TValue*>
        TMarkedPtr(const TMarkedPtr<_TValue>& other, bool marked) noexcept
            : Ptr_(MakeMarkedPtr(other.Get(), marked))
        {}

        TMarkedPtr(const TMarkedPtr& other) = default;
        TMarkedPtr& operator=(const TMarkedPtr&) = default;

        auto operator*() const noexcept
        requires(!CIsVoid<element_type>)
        {
            return *Get();
        }

        element_type* operator->() const noexcept {
            return Get();
        }

        operator element_type*() const noexcept {
            return Get();
        }

        explicit operator bool() const noexcept {
            return Get();
        }

        element_type* Raw() const noexcept {
            return reinterpret_cast<element_type*>(Ptr_);
        }

        element_type* Get() const noexcept {
            return reinterpret_cast<element_type*>(Ptr_ & ~(1));
        }

        void SetMark(bool value) noexcept {
            Ptr_ = MakeMarkedPtr(Get(), value);
        }

        void Mark() noexcept {
            Ptr_ |= 1;
        }

        void Unmark() noexcept {
            Ptr_ &= ~1;
        }

        bool IsMarked() const noexcept {
            return Ptr_ & 1;
        }

        friend std::strong_ordering operator<=>(const TMarkedPtr& left, const TMarkedPtr& right) noexcept {
            return left.Ptr_ <=> right.Ptr_;
        }

        friend bool operator==(const TMarkedPtr& left, const TMarkedPtr& right) noexcept = default;

        template <std::same_as<element_type> TElementType>
        requires(!CIsVoid<element_type>)
        static TMarkedPtr pointer_to(TElementType& value) noexcept { // NOLINT(readability-identifier-naming)
            return TMarkedPtr(std::addressof(value));
        }

    private:
        static inline std::uintptr_t MakeMarkedPtr(element_type* rawPtr, bool marked) noexcept {
            static_assert(CMarkable<element_type>, "aligment of TValue must be greater than 1");
            static constexpr std::uintptr_t mask = 1;
            std::uintptr_t ptr = reinterpret_cast<std::uintptr_t>(rawPtr);
            return ptr | (mask & -std::uintptr_t(marked));
        }

    private:
        std::uintptr_t Ptr_{};
    };

} // namespace NHp::NUtil
