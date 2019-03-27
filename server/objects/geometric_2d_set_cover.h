#pragma once

#include "public.h"

namespace NYP::NServer::NObjects::NGeometric2DSetCover {

////////////////////////////////////////////////////////////////////////////////

//! Covers every point (r, c), where r is in Rows and c is in Columns vectors.
template <class TRow>
struct TRectangle
{
    std::vector<TRow> Rows;
    std::vector<int> Columns;
};

template <class TRow>
using TPoint = std::pair<TRow, int>;

template <class TRow>
bool operator == (const TRectangle<TRow>& lhs, const TRectangle<TRow>& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Each column is supposed to be in a small non-negative range.
template <class TRow>
std::vector<TRectangle<TRow>> BuildPerColumnSetCovering(
    const std::vector<TPoint<TRow>>& points,
    int maxRowsPerRectangle);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects::NGeometric2DSetCover

#define GEOMETRIC_2D_SET_COVER_INL_H_
#include "geometric_2d_set_cover-inl.h"
#undef GEOMETRIC_2D_SET_COVER_INL_H_
