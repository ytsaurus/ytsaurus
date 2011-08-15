#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"
#include "../actions/async_result.h"

namespace NYT
{

///////////////////////////////////////////////////////////////////////////////

// TODO: error handling?
//! Provides a basic interface for readings chunks from holders.
struct IChunkReader
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<IChunkReader> TPtr;

    //! Describes a result of #ReadBlocks.
    struct TReadResult
    {
        //! Blocks data.
        yvector<TSharedRef> Blocks;
    };

    //! Reads (asynchronously) a given set of blocks.
    /*!
     *  Negative indexes indicate that blocks are numbered from the end.
     *  I.e. -1 means the last block.
     */
    virtual TAsyncResult<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes) = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
