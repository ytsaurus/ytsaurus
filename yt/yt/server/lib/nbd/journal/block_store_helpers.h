#pragma once

#include "private.h"

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

struct TParsedBlockId
{
    int ChunkIndex = -1;
    int RecordIndex = -1;
    int FragmentIndex = -1;
};

//! Packs #id into an opaque stored block id.
TStoredBlockId MakeStoredBlockId(TParsedBlockId id);

//! Inverse of MakeStoredBlockId.
TParsedBlockId ParseStoredBlockId(TStoredBlockId blockId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
