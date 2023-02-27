#ifndef CHUNK_LOOKUP_HASH_TABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_lookup_hash_table.h"
// For the sake of sane code completion.
#include "chunk_lookup_hash_table.h"
#endif

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

inline ui64 PackBlockAndRowIndexes(ui16 blockIndex, ui32 rowIndex)
{
    return (static_cast<ui64>(blockIndex) << 32) | rowIndex;
}

inline std::pair<ui16, ui32> UnpackBlockAndRowIndexes(ui64 value)
{
    return {value >> 32, static_cast<ui32>(value)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
