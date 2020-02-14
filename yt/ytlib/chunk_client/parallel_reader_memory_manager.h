#pragma once

#include "public.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Methods used by TSchemalessMergingMultiChunkReader.
struct IMultiReaderMemoryManager
    : public virtual TRefCounted
{
    //! Creates memory manager for particular chunk reader.
    virtual TChunkReaderMemoryManagerPtr CreateChunkReaderMemoryManager(std::optional<i64> reservedMemorySize = std::nullopt) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiReaderMemoryManager)

////////////////////////////////////////////////////////////////////////////////

//! Methods used by ChunkReaderMemoryManager.
struct IReaderMemoryManagerHost
    : public virtual TRefCounted
{
public:
    //! Called by chunk reader memory manager when it is finalized and its usage becomes zero, so we don't need it anymore.
    virtual void Unregister(IReaderMemoryManagerPtr readerMemoryManager) = 0;

    //! Called by chunk reader to notify that memory requirements have changed.
    virtual void UpdateMemoryRequirements(IReaderMemoryManagerPtr readerMemoryManager) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderMemoryManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct TParallelReaderMemoryManagerOptions
{
    TParallelReaderMemoryManagerOptions(
        i64 totalMemorySize,
        i64 maxInitialReaderReservedMemory);

    i64 TotalMemorySize;
    i64 MaxInitialReaderReservedMemory;
};

////////////////////////////////////////////////////////////////////////////////

IMultiReaderMemoryManagerPtr CreateParallelReaderMemoryManager(
    TParallelReaderMemoryManagerOptions options,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
