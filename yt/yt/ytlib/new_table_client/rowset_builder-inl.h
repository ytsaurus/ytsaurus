#pragma once
#ifndef ROWSET_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include rowset_builder.h"
// For the sake of sane code completion.
#include "rowset_builder.h"
#endif
#undef ROWSET_BUILDER_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

inline ui32 TReaderBase::GetKeySegmentsRowLimit(ui32 limit)
{
    for (const auto& column : GetKeyColumns()) {
        limit = std::min(limit, column->GetSegmentRowLimit());
    }
    return limit;
}

inline ui32 TReaderBase::GetValueSegmentsRowLimit(ui32 limit)
{
    for (const auto& column : GetValueColumns()) {
        limit = std::min(limit, column->GetSegmentRowLimit());
    }
    return limit;
}

inline void TReaderBase::CollectCounts(ui32* valueCounts, TRange<TReadSpan> spans)
{
    if (spans.Empty()) {
        return;
    }

    ui16 id = GetKeyColumnCount();
    for (const auto& column : GetValueColumns()) {
        Positions_[id] = column->CollectCounts(valueCounts, spans, Positions_[id]);
        ++id;
    }
}

inline ui32 TReaderBase::GetKeyColumnCount() const
{
    return KeyColumns_.size();
}

template <class T>
inline T* TReaderBase::Allocate(size_t size)
{
    return reinterpret_cast<T*>(GetPool()->AllocateAligned(sizeof(T) * size));
}

inline TChunkedMemoryPool* TReaderBase::GetPool() const
{
    return Buffer_->GetPool();
}

inline void TReaderBase::ClearBuffer()
{
    Buffer_->Clear();
}

inline TRange<std::unique_ptr<TKeyColumnBase>> TReaderBase::GetKeyColumns() const
{
    return KeyColumns_;
}

inline TRange<std::unique_ptr<TVersionedValueColumnBase>> TReaderBase::GetValueColumns() const
{
    return ValueColumns_;
}

inline void TReaderBase::ResetPosition(ui16 id)
{
    Positions_[id] = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
