#pragma once
#ifndef UNVERSIONED_ROW_BATCH_INL_H_
#error "Direct inclusion of this file is not allowed, include unversioned_row_batch.h"
// For the sake of sane code completion.
#include "unversioned_row_batch.h"
#endif
#undef UNVERSIONED_ROW_BATCH_INL_H_

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRange<T> IUnversionedColumnarRowBatch::TColumn::GetTypedValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == sizeof(T) * 8);
    return TRange<T>(
        reinterpret_cast<const T*>(Values->Data.Begin()),
        reinterpret_cast<const T*>(Values->Data.End()));
}


template <class T>
TRange<T> IUnversionedColumnarRowBatch::TColumn::GetRelevantTypedValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == sizeof(T) * 8);
    YT_VERIFY(!Rle);
    return TRange<T>(
        reinterpret_cast<const T*>(Values->Data.Begin()) + StartIndex,
        reinterpret_cast<const T*>(Values->Data.Begin()) + StartIndex + ValueCount);
}

inline TRef IUnversionedColumnarRowBatch::TColumn::GetBitmapValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == 1);
    YT_VERIFY(Values->BaseValue == 0);
    YT_VERIFY(!Values->ZigZagEncoded);
    YT_VERIFY(!Rle);
    YT_VERIFY(!Dictionary);
    return Values->Data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient