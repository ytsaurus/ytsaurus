#pragma once

#include <type_traits>
#include <utility>

namespace NLockFreeMap::NPrivate {

    template <class TKey>
    struct TSetKeySelect {
        using TType = TKey;

        template <class T, class = std::enable_if_t<std::is_same_v<std::remove_cvref_t<T>, TType>>>
        const T& operator()(const T& value) const noexcept {
            return value;
        }
    };

    template <class TKey>
    struct TMapKeySelect {
        using TType = TKey;

        template <class T1, class T2, class = std::enable_if_t<std::is_same_v<std::remove_cvref_t<T1>, TKey>>>
        const TKey& operator()(const std::pair<T1, T2>& value) const {
            return value.first;
        }

        template <class T1, class T2, class = std::enable_if_t<std::is_same_v<std::remove_cvref_t<T1>, TKey>>>
        const TKey& operator()(const T1& key, const T2&) const {
            return key;
        }
    };

} // namespace NLockFreeMap::NPrivate
