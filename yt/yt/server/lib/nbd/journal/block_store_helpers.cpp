#include "block_store_helpers.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

TStoredBlockId MakeStoredBlockId(TParsedBlockId id)
{
    using namespace NStoredBlockIdLayout;
    YT_VERIFY(0 <= id.ChunkIndex && id.ChunkIndex < MaxChunksPerDevice);
    YT_VERIFY(0 <= id.RecordIndex && id.RecordIndex < MaxRecordsPerChunk);
    YT_VERIFY(0 <= id.FragmentIndex && id.FragmentIndex < MaxFragmentsPerRecord);
    return TStoredBlockId(
        (static_cast<ui64>(id.ChunkIndex) << (RecordIndexBits + FragmentIndexBits)) |
        (static_cast<ui64>(id.RecordIndex) << FragmentIndexBits) |
        static_cast<ui64>(id.FragmentIndex));
}

TParsedBlockId ParseStoredBlockId(TStoredBlockId blockId)
{
    using namespace NStoredBlockIdLayout;
    auto value = blockId.Underlying();
    return {
        .ChunkIndex = static_cast<int>((value >> (RecordIndexBits + FragmentIndexBits)) & (MaxChunksPerDevice - 1)),
        .RecordIndex = static_cast<int>((value >> FragmentIndexBits) & (MaxRecordsPerChunk - 1)),
        .FragmentIndex = static_cast<int>(value & (MaxFragmentsPerRecord - 1)),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
