#pragma once

#include "chunk.pb.h"

#include "../misc/common.h"
#include "../misc/ref.h"
#include "../actions/future.h"
#include "../misc/enum.h"

#include "../misc/error.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for readings chunks from holders.
struct IAsyncReader
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IAsyncReader> TPtr;

    //! Describes a result of #AsyncReadBlocks.
    typedef TValuedError< yvector<TSharedRef> > TReadResult;

    //! Describes a result of #AsyncGetChunkInfo.
    typedef TValuedError<NChunkHolder::NProto::TChunkInfo> TGetInfoResult;

    //! Reads (asynchronously) a given set of blocks.
    /*!
     *  Negative indexes indicate that blocks are numbered from the end.
     *  I.e. -1 means the last block.
     */
    virtual TFuture<TReadResult>::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes) = 0;

    virtual TFuture<TGetInfoResult>::TPtr AsyncGetChunkInfo() = 0;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
