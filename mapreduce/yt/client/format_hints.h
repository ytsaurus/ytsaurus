#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/generic/maybe.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <typename TRow>
void ApplyFormatHints(TFormat* format, const TMaybe<TFormatHints>& formatHints);

// NOTE: only TNode is implemented for now since it's the only representation that has implemented hints.
template <>
void ApplyFormatHints<TNode>(TFormat* format, const TMaybe<TFormatHints>& formatHints);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
