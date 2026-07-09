#pragma once

#include <memory>

namespace NHp::NIntrusive::NPrivate {

    template <class TTo, class TFrom>
    concept CHasStaticCastFrom = (
        requires(TFrom ptr) { {TTo::StaticCastFrom(ptr)} -> std::same_as<TTo>; } &&
        requires(const TFrom& ptr) { {TTo::StaticCastFrom(ptr)} -> std::same_as<TTo>; });

    template <class TTo, class TFrom>
    concept CHasConstCastFrom = (
        requires(TFrom ptr) { {TTo::ConstCastFrom(ptr)} -> std::same_as<TTo>; } &&
        requires(const TFrom& ptr) { {TTo::ConstCastFrom(ptr)} -> std::same_as<TTo>; });

    template <class TTo, class TFrom>
    concept CHasDynamicCastFrom = (
        requires(TFrom ptr) { {TTo::DynamicCastFrom(ptr)} -> std::same_as<TTo>; } &&
        requires(const TFrom& ptr) { {TTo::DynamicCastFrom(ptr)} -> std::same_as<TTo>; });

    template <class _TPointer>
    struct TPointerCastTraits {
        using TPointer = _TPointer;
        using TPointerTraits = std::pointer_traits<TPointer>;
        using TElementType = typename TPointerTraits::element_type;

        template <class TToPointer>
        static TPointer StaticCastFrom(const TToPointer& uptr) noexcept {
            if constexpr (CHasConstCastFrom<TPointer, TToPointer>) {
                return TPointer::StaticCastFrom(uptr);
            } else {
                if (uptr) {
                    return TPointerTraits::pointer_to(static_cast<TElementType&>(*uptr));
                }
                return TPointer{};
            }
        }

        template <class TToPtr>
        static TPointer ConstCastFrom(const TToPtr& uptr) noexcept {
            if constexpr (CHasConstCastFrom<TPointer, TToPtr>) {
                return TPointer::ConstCastFrom(uptr);
            } else {
                if (uptr) {
                    return TPointerTraits::pointer_to(const_cast<TElementType&>(*uptr));
                }
                return TPointer{};
            }
        }

        template <class TToPtr>
        static TPointer DynamicCastFrom(const TToPtr& uptr) noexcept {
            if constexpr (CHasDynamicCastFrom<TPointer, TToPtr>) {
                return TPointer::DynamicCastFrom(uptr);
            } else {
                if (uptr) {
                    return TPointerTraits::pointer_to(dynamic_cast<TElementType&>(*uptr));
                }
                return TPointer{};
            }
        }
    };

    template <class T>
    struct TPointerCastTraits<T*> {
        using TPointer = T*;
        using TPointerTraits = std::pointer_traits<TPointer>;
        using TElementType = typename TPointerTraits::element_type;

        template <class UPtr>
        static TPointer StaticCastFrom(UPtr* uptr) noexcept {
            return static_cast<TPointer>(uptr);
        }

        template <class UPtr>
        static TPointer ConstCastFrom(UPtr* uptr) noexcept {
            return const_cast<TPointer>(uptr);
        }

        template <class UPtr>
        static TPointer DynamicCastFrom(UPtr* uptr) noexcept {
            return dynamic_cast<TPointer>(uptr);
        }
    };

} // namespace NHp::NIntrusive::NPrivate
