#pragma once

#include "fwd.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TDontRequireSerializeTag
{ };

template <typename T>
struct TIsManuallyNonCodable : public std::false_type
{ };

template <std::derived_from<TDontRequireSerializeTag> T>
struct TIsManuallyNonCodable<T> : public std::true_type
{ };

template <typename T>
concept CManuallyNonCodable = TIsManuallyNonCodable<T>::value;

template <typename T>
concept CImplicitlyCodable = (!CManuallyNonCodable<T>);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
