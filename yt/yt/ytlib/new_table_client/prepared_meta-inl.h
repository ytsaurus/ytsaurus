#pragma once
#ifndef PREPARED_META_INL_H_
#error "Direct inclusion of this file is not allowed, include prepared_meta.h"
// For the sake of sane code completion.
#include "prepared_meta.h"
#endif
#undef PREPARED_META_INL_H_

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

bool IsDirect(int type)
{
    // DirectRle/DirectSparse: 2,  DirectDense: 3
    return type == 2 || type == 3;
}

bool IsDense(int type)
{
    // DictionaryDense: 1, DirectDense: 3
    return type == 1 || type == 3;
}

template <EValueType Type>
void TValueMeta<Type>::Init(const NProto::TSegmentMeta& meta)
{
    TMeta<Type>::Init(meta);
    TDenseMeta::Init(meta);
}

template <EValueType Type>
void TKeyMeta<Type>::Init(const NProto::TSegmentMeta& meta)
{
    TMeta<Type>::Init(meta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
