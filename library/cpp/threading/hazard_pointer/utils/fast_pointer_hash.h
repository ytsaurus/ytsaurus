#pragma once

#include <cstddef>
#include <type_traits>

namespace NHp::NUtil {

    template <std::size_t argument, std::size_t base = 2, bool = (argument < base)>
    static constexpr std::size_t Log = 1 + Log<argument / base, base>;

    template <std::size_t argument, std::size_t base>
    static constexpr std::size_t Log<argument, base, true> = 0;

    struct TFastPointerHash {
        template <class T>
        std::size_t operator()(T* p) const noexcept {
            auto hash = reinterpret_cast<std::size_t>(p);
            if constexpr (!std::is_void_v<T>) {
                hash >>= Log<alignof(T)>;
            }
            return hash;
        }
    };

} // namespace NHp::NUtil
