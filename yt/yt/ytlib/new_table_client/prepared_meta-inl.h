#ifndef PREPARED_META_INL_H_
#error "Direct inclusion of this file is not allowed, include prepared_meta.h"
// For the sake of sane code completion.
#include "prepared_meta.h"
#endif
#undef PREPARED_META_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

bool TMultiValueIndexMeta::IsDense() const
{
    return ExpectedPerRow != static_cast<ui32>(-1);
}

template <EValueType Type>
void TValueMeta<Type>::Init(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::Init(meta, ptr, false);
    TDataMeta<Type>::Init(meta, ptr);
}

template <EValueType Type>
void TAggregateValueMeta<Type>::Init(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::Init(meta, ptr, true);
    TDataMeta<Type>::Init(meta, ptr);
}

template <EValueType Type>
void TKeyMeta<Type>::Init(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = TKeyIndexMeta::Init(meta, Type, ptr);
        TDataMeta<Type>::Init(meta, ptr);
    } else {
        ptr = TDataMeta<Type>::Init(meta, ptr);
        TKeyIndexMeta::Init(meta, Type, ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
