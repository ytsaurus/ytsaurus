#pragma once

#include <util/generic/typelist.h>

namespace NHp::NOptions::NPrivate {

    template <template <class> class TTargetOption, class TDefault, class TOptions>
    struct TGetOptionTypeImpl;

    template <template <class> class TTargetOption, class TDefault, class TOption, class... TOptions>
    struct TGetOptionTypeImpl<TTargetOption, TDefault, TTypeList<TOption, TOptions...>> {
        using TType = typename TGetOptionTypeImpl<TTargetOption, TDefault, TTypeList<TOptions...>>::TType;
    };

    template <template <class> class TTargetOption, class TDefault, class TOption, class... TOptions>
    struct TGetOptionTypeImpl<TTargetOption, TDefault, TTypeList<TTargetOption<TOption>, TOptions...>> {
        using TType = TOption;
    };

    template <template <class> class TTargetOption, class TDefault>
    struct TGetOptionTypeImpl<TTargetOption, TDefault, TTypeList<>> {
        using TType = TDefault;
    };

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default, class TOptions>
    struct TGetOptionValueImpl;

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default, class TOption, class... TOptions>
    struct TGetOptionValueImpl<TValueType, TTargetOption, Default, TTypeList<TOption, TOptions...>> {
        static constexpr TValueType Value = TGetOptionValueImpl<TValueType, TTargetOption, Default, TTypeList<TOptions...>>::Value;
    };

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default, TValueType Option, class... TOptions>
    struct TGetOptionValueImpl<TValueType, TTargetOption, Default, TTypeList<TTargetOption<Option>, TOptions...>> {
        static constexpr TValueType Value = Option;
    };

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default>
    struct TGetOptionValueImpl<TValueType, TTargetOption, Default, TTypeList<>> {
        static constexpr TValueType Value = Default;
    };

} // namespace NHp::NOptions::NPrivate

namespace NHp::NOptions {

    template <template <class> class TTargetOption, class TDefault, class... TOptions>
    struct TGetOptionType {
        using TType = typename NPrivate::TGetOptionTypeImpl<TTargetOption, TDefault, TTypeList<TOptions...>>::TType;
    };

    template <template <class> class TTargetOption, class TDefault, class... TOptions>
    using TOptionType =
        typename TGetOptionType<TTargetOption, TDefault, TOptions...>::TType;

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default, class... TOptions>
    struct TGetOptionValue {
        static constexpr TValueType Value = NPrivate::TGetOptionValueImpl<TValueType, TTargetOption, Default, TTypeList<TOptions...>>::Value;
    };

    template <class TValueType, template <TValueType> class TTargetOption, TValueType Default, class... TOptions>
    inline constexpr TValueType TOptionValue =
        TGetOptionValue<TValueType, TTargetOption, Default, TOptions...>::Value;

} // namespace NHp::NOptions
