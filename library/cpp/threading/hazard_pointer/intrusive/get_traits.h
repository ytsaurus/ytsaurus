#pragma once

#include "base_value_traits.h"

namespace NHp::NIntrusive::NPrivate {

    template <class T, class TValue>
    concept CIsDefaultHook = requires {
        typename T::template TGetDefaultHook<TValue>::TType;
    };

    template <class T>
    concept CIsHook = requires { typename T::THookTags; };

    template <class T>
    concept CHasNodeTraits = requires { typename T::TNodeTraits; };

    template <class TValue, class THookType>
    struct THookToValueTraits {
        using THookTags = typename THookType::THookTags;
        using TType = NPrivate::TBaseValueTraits<
            TValue,
            typename THookTags::TNodeTraits,
            typename THookTags::TTag,
            THookTags::IsAutoUnlink>;
    };

    template <class TValue, class TProtoValueTraits>
    struct TGetValueTraits {
        using TType = TProtoValueTraits;
    };

    template <class TValue, CIsDefaultHook<TValue> TProtoValueTraits>
    struct TGetValueTraits<TValue, TProtoValueTraits> {
        using THookType = typename TProtoValueTraits::template TGetDefaultHook<TValue>::TType;
        using TType = typename THookToValueTraits<TValue, THookType>::TType;
    };

    template <class TValue, CIsHook TProtoValueTraits>
    struct TGetValueTraits<TValue, TProtoValueTraits> {
        using TType = typename THookToValueTraits<TValue, TProtoValueTraits>::TType;
    };

    template <class TProtoValueTraits>
    struct TGetNodeTraits;

    template <CIsHook TProtoValueTraits>
    struct TGetNodeTraits<TProtoValueTraits> {
        using TType = typename TProtoValueTraits::THookTags::TNodeTraits;
    };

    template <CHasNodeTraits TProtoValueTraits>
    struct TGetNodeTraits<TProtoValueTraits> {
        using TType = typename TProtoValueTraits::TNodeTraits;
    };

} // namespace NHp::NIntrusive::NPrivate
