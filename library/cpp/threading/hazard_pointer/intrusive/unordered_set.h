#pragma once

#include "get_traits.h"
#include "hashtable.h"
#include "options.h"

#include <library/cpp/threading/hazard_pointer/options/get_option.h>

#include <util/generic/fwd.h>

namespace NHp::NIntrusive::NPrivate {

    template <class TValue>
    struct TDefaultKeyOfValue {
        template <class T, class = std::enable_if_t<std::is_same_v<std::remove_cvref_t<T>, TValue>>>
        T&& operator()(T&& value) const noexcept {
            return std::forward<T>(value);
        }
    };

    template <class TProtoBucketTraits, class, class>
    struct TGetBucketTraits {
        using TType = TProtoBucketTraits;
    };

    template <class TValueTraits, class TSizeType>
    struct TGetBucketTraits<void, TValueTraits, TSizeType> {
        using TBucket = TBucketValue<typename TValueTraits::TNodeTraits, false>;
        using TBucketPtr = typename std::pointer_traits<
            typename TValueTraits::TPointer>::template rebind<TBucket>;
        using TType = TBucketTraitsImpl<TBucketPtr, TSizeType>;
    };

    template <class... TOptions>
    struct TMakeUnorderedSetBaseHook {
        using TVoidPtr = NOptions::TOptionType<NOptions::TVoidPointer, void*, TOptions...>;
        using TTag = NOptions::TOptionType<NOptions::TTag, TDefaultHookTag, TOptions...>;
        static constexpr bool StoreHash = NOptions::TOptionValue<bool, NOptions::TStoreHash, true, TOptions...>;
        static constexpr bool IsAutoUnlink = NOptions::TOptionValue<bool, NOptions::TIsAutoUnlink, true, TOptions...>;

        using TType = THashTableBaseHook<TVoidPtr, TTag, StoreHash, IsAutoUnlink>;
    };

    template <class TValue, bool IsMulti, class... TOptions>
    struct TMakeUnorderedSet {
        struct TDefaultProtoValueTraits {
            template <class _TValue>
            struct TGetDefaultHook {
                using TType = typename _TValue::_THashTableDefaultHook;
            };
        };

        using TKeyOfValue = NOptions::TOptionType<NOptions::TKeyOfValue, TDefaultKeyOfValue<TValue>, TOptions...>;
        using TKey = typename TGetKey<TKeyOfValue, TValue>::TType;

        using THasher = NOptions::TOptionType<NOptions::THasher, THash<TKey>, TOptions...>;
        using TKeyEqual = NOptions::TOptionType<NOptions::TEqual, TEqualTo<TKey>, TOptions...>;

        using TSizeType = NOptions::TOptionType<NOptions::TSizeType, std::size_t, TOptions...>;

        using TProtoValueTraits = NOptions::TOptionType<
            NOptions::TValueTraits,
            NOptions::TOptionType<NOptions::TBaseHook, TDefaultProtoValueTraits, TOptions...>,
            TOptions...>;

        using TValueTraits = typename TGetValueTraits<TValue, TProtoValueTraits>::TType;

        using TProtoBucketTraits = NOptions::TOptionType<NOptions::TBucketTraits, void, TOptions...>;
        using TBucketTraits = typename TGetBucketTraits<TProtoBucketTraits, TValueTraits, TSizeType>::TType;

        static constexpr bool IsPower2Buckets = NOptions::TOptionValue<bool, NOptions::TIsPower2Buckets, false, TOptions...>;

        using TType = TIntrusiveHashTable<TValueTraits, TBucketTraits, TKeyOfValue, THasher, TKeyEqual, TSizeType, IsPower2Buckets, IsMulti>;
    };

    template <class... TOptions>
    struct TMakeUnorderedBucketType {
        using TProtoValueTraits = NOptions::TOptionType<
            NOptions::TValueTraits,
            NOptions::TOptionType<NOptions::TBaseHook, void, TOptions...>,
            TOptions...>;

        static_assert(!std::is_void_v<TProtoValueTraits>, "TBaseHook or TValueTraits must be specified");
        using TNodeTraits = typename TGetNodeTraits<TProtoValueTraits>::TType;
        using TType = TBucketValue<TNodeTraits, false>;
    };

} // namespace NHp::NIntrusive::NPrivate

namespace NHp::NIntrusive {

    template <class... TOptions>
    using TUnorderedSetBaseHook = typename NPrivate::TMakeUnorderedSetBaseHook<TOptions...>::TType;

    template <class TValue, class... TOptions>
    using TUnorderedSet = typename NPrivate::TMakeUnorderedSet<TValue, false, TOptions...>::TType;

    template <class TValue, class... TOptions>
    using TUnorderedMultiSet = typename NPrivate::TMakeUnorderedSet<TValue, true, TOptions...>::TType;

    template <class... TOptions>
    using TUnorderedBucketType = typename NPrivate::TMakeUnorderedBucketType<TOptions...>::TType;

} // namespace NHp::NIntrusive
