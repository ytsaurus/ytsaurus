#pragma once

#include "fwd.h"

#include <type_traits>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TMultiRow
{
    TMultiRow() = delete;
};

template <typename T>
concept CMultiRow = std::is_same_v<T, TMultiRow>;

template <typename T>
concept CRow = !CMultiRow<T> && (std::same_as<std::decay_t<T>, T> || std::same_as<std::decay_t<T>&&, T>);

template <typename TItem>
concept CPCollection = TIsSpecializationOf<TPCollection, TItem>::value;

template <typename T>
class TIsPCollectionTuple : public std::false_type {};

template <CPCollection... TItems>
class TIsPCollectionTuple<std::tuple<TItems...>> : public std::true_type {};

template <typename TItem>
concept CPCollectionTuple = TIsPCollectionTuple<TItem>::value;

template <typename TItem>
concept CRorenGraphItem =
    std::is_same_v<TItem, void> ||
    std::is_same_v<TItem, TMultiPCollection> ||
    CPCollection<TItem> ||
    CPCollectionTuple<TItem>;

template <typename TTransform, typename TInput>
concept CApplicableTo = requires(const TTransform& transform, const TInput& input)
{
    { transform.ApplyTo(input) } -> CRorenGraphItem;
    { transform.GetName() } -> std::same_as<TString>;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
