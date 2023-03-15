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
concept CRow = !CMultiRow<T>;

template <typename TItem>
concept CRorenGraphItem =
    std::is_same_v<TItem, void> ||
    std::is_same_v<TItem, TMultiPCollection> ||
    TIsSpecializationOf<TPCollection, TItem>::value;

template <typename TTransform, typename TInput>
concept CApplicableTo = requires(const TTransform& a, const TInput& input)
{
    { a.ApplyTo(input) } -> CRorenGraphItem;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
