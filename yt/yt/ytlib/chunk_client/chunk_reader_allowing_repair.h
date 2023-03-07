#pragma once

#include "chunk_reader.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! An interface for reading chunks with possibility to fail on slow reads.
struct IChunkReaderAllowingRepair
    : public IChunkReader
{
    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)> slownessChecker) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkReaderAllowingRepair)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
