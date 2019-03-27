#pragma once
#ifndef GEOMETRIC_2D_SET_COVER_INL_H_
#error "Direct inclusion of this file is not allowed, include geometric_2d_set_cover.h"
// For the sake of sane code completion.
#include "geometric_2d_set_cover.h"
#endif

#include <util/generic/bitmap.h>
#include <util/generic/hash.h>

namespace NYP::NServer::NObjects::NGeometric2DSetCover {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

std::vector<int> GetNonZeroBits(const TDynBitMap& bitmap);

// We are using ordered collection as a result to preserve rows order.
template <class TRow>
std::map<TRow, TDynBitMap> BuildRowBitmaps(const std::vector<TPoint<TRow>>& points)
{
    std::map<TRow, TDynBitMap> rowBitmaps;
    for (const auto& point : points) {
        rowBitmaps[point.first].Set(point.second);
    }
    return rowBitmaps;
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
bool operator == (const TRectangle<TRow>& lhs, const TRectangle<TRow>& rhs)
{
    return lhs.Rows == rhs.Rows && lhs.Columns == rhs.Columns;
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
std::vector<TRectangle<TRow>> BuildPerColumnSetCovering(
    const std::vector<TPoint<TRow>>& points,
    int maxRowsPerRectangle)
{
    YCHECK(maxRowsPerRectangle > 0);

    for (const auto& point : points) {
        YCHECK(point.second >= 0);
    }

    auto rowBitmaps = NPrivate::BuildRowBitmaps(points);

    std::vector<TRectangle<TRow>> result;
    THashMap<TDynBitMap, TRectangle<TRow>> incompleteRectangles;
    for (const auto& [row, rowBitmap] : rowBitmaps) {
        auto it = incompleteRectangles.find(rowBitmap);
        if (it == incompleteRectangles.end()) {
            it = incompleteRectangles.emplace(rowBitmap, TRectangle<TRow>()).first;
            auto& rectangle = it->second;
            rectangle.Columns = NPrivate::GetNonZeroBits(rowBitmap);
        }
        auto& rectangle = it->second;
        rectangle.Rows.push_back(row);
        if (static_cast<int>(rectangle.Rows.size()) >= maxRowsPerRectangle) {
            auto& resultRectangle = result.emplace_back();
            std::swap(resultRectangle.Rows, rectangle.Rows);
            resultRectangle.Columns = rectangle.Columns;
        }
    }

    for (auto& [bitmap, rectangle] : incompleteRectangles) {
        if (!rectangle.Rows.empty()) {
            result.push_back(std::move(rectangle));
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects::NGeometric2DSetCover
