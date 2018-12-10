#pragma once
#ifndef CHUNK_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_list.h"
// For the sake of sane code completion.
#include "chunk_list.h"
#endif

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

inline ui64 TChunkList::GetVisitMark() const
{
    return GetDynamicData()->VisitMark;
}

inline void TChunkList::SetVisitMark(ui64 value)
{
    GetDynamicData()->VisitMark = value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
