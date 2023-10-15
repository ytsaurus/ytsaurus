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
void TValueMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::InitFromProto(meta, ptr, false);
    TDataMeta<Type>::InitFromProto(meta, ptr);
}

template <EValueType Type>
void TAggregateValueMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::InitFromProto(meta, ptr, true);
    TDataMeta<Type>::InitFromProto(meta, ptr);
}

template <EValueType Type>
void TKeyMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = TKeyIndexMeta::InitFromProto(meta, Type, ptr);
        TDataMeta<Type>::InitFromProto(meta, ptr);
    } else {
        ptr = TDataMeta<Type>::InitFromProto(meta, ptr);
        TKeyIndexMeta::InitFromProto(meta, Type, ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
