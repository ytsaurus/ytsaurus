#pragma once

#include "chunk.pb.h"

#include "../misc/common.h"
#include "../misc/ref.h"
#include "../actions/future.h"
#include "../misc/enum.h"
#include "../rpc/error.h"

#include "../misc/error.h"

namespace NYT {
namespace NChunkClient {

using NChunkServer::NProto::TChunkInfo;

///////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for readings chunks from holders.
struct IAsyncReader
    : virtual public TRefCountedBase
{
    typedef TIntrusivePtr<IAsyncReader> TPtr;

    //ToDo: pretty heavy obj. May be RefCounted?
    //! Describes a result of #ReadBlocks.
    struct TReadResult
    {
        //! Blocks data.
        yvector<TSharedRef> Blocks;
        TError Error;
    };

    //! Describes a result of #GetChunkInfo
    struct TGetInfoResult
    {
        TChunkInfo ChunkInfo;
        TError Error;
    };

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
