#pragma once
#ifndef CHUNK_LIST_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_list.h"
#endif

namespace NYT {
namespace NChunkServer {

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

} // namespace NChunkServer
} // namespace NYT
