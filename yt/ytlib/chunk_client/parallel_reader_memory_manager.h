#pragma once

#include "chunk_reader_memory_manager.h"

#include <yt/core/profiling/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Methods used by TSchemalessMergingMultiChunkReader.
struct IMultiReaderMemoryManager
    : public IReaderMemoryManager
{
    //! Returns amount of free memory in memory manager.
    virtual i64 GetFreeMemorySize() = 0;

    //! Creates memory manager for particular chunk reader with `reservedMemorySize' reserved memory.
    //! If not set `MaxInitialReaderReservedMemory' memory will be allocated.
    virtual TChunkReaderMemoryManagerPtr CreateChunkReaderMemoryManager(
        std::optional<i64> reservedMemorySize = std::nullopt,
        NProfiling::TTagIdList profilingTagList = {}) = 0;

    //! Creates child multi reader memory manager with `requiredMemorySize' reserved memory. Memory requirements of child memory manager will
    //! never become less than `requiredMemorySize' until its destruction.
    //! If not set it is assumed to be equal 0.
    virtual IMultiReaderMemoryManagerPtr CreateMultiReaderMemoryManager(
        std::optional<i64> requiredMemorySize = std::nullopt,
        NProfiling::TTagIdList profilingTagList = {}) = 0;
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

    virtual TGuid GetId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderMemoryManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct TParallelReaderMemoryManagerOptions
{
    TParallelReaderMemoryManagerOptions(
        i64 totalReservedMemorySize,
        i64 maxInitialReaderReservedMemory,
        i64 minRequiredMemorySize,
        NProfiling::TTagIdList profilingTagList = {},
        bool enableDetailedLogging = false,
        bool enableProfiling = false,
        TDuration profilingPeriod = TDuration::Seconds(1));

    //! Amount of memory reserved for this memory manager at the moment of creation.
    //! This amount can be changed later using `SetReservedMemorySize' call.
    i64 TotalReservedMemorySize;

    //! Maximum (and default) amount of reserved memory for created reader.
    i64 MaxInitialReaderReservedMemory;

    //! Required memory size of memory manager will never become less than this value until
    //! destruction.
    i64 MinRequiredMemorySize;

    const NProfiling::TTagIdList ProfilingTagList;

    bool EnableProfiling = false;

    TDuration ProfilingPeriod = TDuration::Seconds(5);

    bool EnableDetailedLogging = false;
};

////////////////////////////////////////////////////////////////////////////////

IMultiReaderMemoryManagerPtr CreateParallelReaderMemoryManager(
    TParallelReaderMemoryManagerOptions options,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
