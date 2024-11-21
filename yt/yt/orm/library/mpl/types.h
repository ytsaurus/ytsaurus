#pragma once

#include "public.h"

#include <concepts>
#include <cstddef>

namespace NYT::NOrm::NMpl {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <size_t Index, class... Ts>
struct TTypesGetImpl;

template <class THead, class... TTail>
struct TTypesGetImpl<0, THead, TTail...>
{
    using T = THead;
};

template <size_t Index, class THead, class... TTail>
struct TTypesGetImpl<Index, THead, TTail...>
{
    using T = TTypesGetImpl<Index - 1, TTail...>::T;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Ts>
struct TTypesIndexOfImpl;

template <class T, class... Ts>
struct TTypesIndexOfImpl<T, T, Ts...>
{
    static constexpr size_t Index = 0;
};

template <class T, class THead, class... TTail>
struct TTypesIndexOfImpl<T, THead, TTail...>
{
    static constexpr size_t Index = TTypesIndexOfImpl<T, TTail...>::Index + 1;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Ts>
concept CInvocableForTypes = (requires (T&& invokable) {
    { invokable.template operator()<Ts>() };
} && ...);

template <class T, class... Ts>
concept CStaticPredicate = (requires {
    { T{}.template operator()<Ts>() } -> std::same_as<bool>;

    // NB! Ad-hoc way to check that predicate can be evaluated in compile-time.
    { std::conditional_t<T{}.template operator()<Ts>(), int, double>{} };
} && ...);

////////////////////////////////////////////////////////////////////////////////

template <class TPredicate, class... Ts>
struct TTypesSuchThatImpl;

template <class TPredicate, class THead, class... TTail>
struct TTypesSuchThatImpl<TPredicate, THead, TTail...>
{
    using T = TTypesSuchThatImpl<TPredicate, TTail...>::T;
};

template <class TPredicate, class THead, class... TTail>
    requires (TPredicate{}.template operator()<THead>())
struct TTypesSuchThatImpl<TPredicate, THead, TTail...>
{
    using T = THead;
};

////////////////////////////////////////////////////////////////////////////////

template <template <class...> class TWrapper, class... TFiltered>
struct TTypesFilterImpl
{
    template <class TPredicate, class... Ts>
    struct TRun
    {
        using T = TWrapper<TFiltered...>;
    };

    template <class TPredicate, class THead, class... TTail>
    struct TRun<TPredicate, THead, TTail...>
    {
        using T = TRun<TPredicate, TTail...>::T;
    };

    template <class TPredicate, class THead, class... TTail>
        requires (TPredicate{}.template operator()<THead>())
    struct TRun<TPredicate, THead, TTail...>
    {
        using T = TTypesFilterImpl<TWrapper, TFiltered..., THead>
            ::template TRun<TPredicate, TTail...>::T;
    };
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class... Ts>
struct TTypes
{
    static constexpr size_t Size = sizeof...(Ts);

    static constexpr bool Distinct = CDistinct<Ts...>;

    template <size_t Index>
    requires (0 <= Index && Index < Size)
    using Get = NDetail::TTypesGetImpl<Index, Ts...>;

    template <class TNiddle>
    static constexpr bool Contains = COneOf<TNiddle, Ts...>;

    template <COneOf<Ts...> TNiddle>
    static constexpr size_t IndexOf = NDetail::TTypesIndexOfImpl<TNiddle, Ts...>::Index;

    template <class TInvocable>
    static constexpr bool InvocableForEachType = NDetail::CInvocableForTypes<TInvocable, Ts...>;

    template <template <class...> class TWrapper>
    using Wrap = TWrapper<Ts...>;

    template <template <class> class TMapper>
    using Map = TTypes<TMapper<Ts>...>;

    template <NDetail::CStaticPredicate<Ts...> TPredicate>
    using Filter = NDetail::TTypesFilterImpl<TTypes>::TRun<TPredicate, Ts...>;

    template <NDetail::CStaticPredicate<Ts...> TPredicate>
    using SuchThat = NDetail::TTypesSuchThatImpl<TPredicate, Ts...>::T;

    template <NDetail::CStaticPredicate<Ts...> TPredicate>
    static constexpr bool All = (TPredicate{}.template operator()<Ts>() && ...);

    template <NDetail::CStaticPredicate<Ts...> TPredicate>
    static constexpr bool Any = (TPredicate{}.template operator()<Ts>() || ...);

    template <NDetail::CInvocableForTypes<Ts...> TInvocable>
    static constexpr void ForEach(const TInvocable& invocable)
    {
        (invocable.template operator()<Ts>(), ...);
    }

    template <template <class...> class TResult, NDetail::CInvocableForTypes<Ts...> TProducer>
    static constexpr auto Produce(const TProducer& producer)
    {
        return TResult(producer.template operator()<Ts>()...);
    }

    struct TUnion
        : public virtual Ts...
    { };
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CTypes = IsSpecialization<T, TTypes>;

template <class T>
concept CDistinctTypes = CTypes<T> && T::Distinct;

template <class T, class TObjects>
concept COneOfTypes = CTypes<TObjects> && TObjects::template Contains<T>;

template <class TObjects, class TPredicate>
concept CAllTypes = CTypes<TObjects> && TObjects::template All<TPredicate>;

template <class TObjects, class TPredicate>
concept CAnyTypes = CTypes<TObjects> && TObjects::template Any<TPredicate>;

template <class T, class TObjects>
concept CInvocableForEachType = CTypes<TObjects> && TObjects::template InvocableForEachType<T>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NMpl
