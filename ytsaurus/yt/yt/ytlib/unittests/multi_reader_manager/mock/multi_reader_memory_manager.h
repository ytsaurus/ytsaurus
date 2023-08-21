#pragma once

#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

namespace NYT::NChunkClient {

// TODO(max42): move to .cpp

////////////////////////////////////////////////////////////////////////////////

class TMultiReaderMemoryManagerMock
    : public IMultiReaderMemoryManager
    , public IReaderMemoryManagerHost
{
public:
    TChunkReaderMemoryManagerPtr CreateChunkReaderMemoryManager(
        std::optional<i64> /*reservedMemorySize*/,
        const NProfiling::TTagList& /*profilingTagList*/) override
    {
        YT_UNIMPLEMENTED();
    }

    IMultiReaderMemoryManagerPtr CreateMultiReaderMemoryManager(
        std::optional<i64> /*requiredMemorySize*/,
        const NProfiling::TTagList& /*profilingTagList*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Unregister(IReaderMemoryManagerPtr /*readerMemoryManager*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void UpdateMemoryRequirements(IReaderMemoryManagerPtr /*readerMemoryManager*/) override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetRequiredMemorySize() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetDesiredMemorySize() const override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetReservedMemorySize() const override
    {
        YT_UNIMPLEMENTED();
    }

    void SetReservedMemorySize(i64 /*size*/) override
    {
        YT_UNIMPLEMENTED();
    }

    i64 GetFreeMemorySize() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> Finalize() override
    {
        return VoidFuture;
    }

    const NProfiling::TTagList& GetProfilingTagList() const override
    {
        YT_UNIMPLEMENTED();
    }

    void AddChunkReaderInfo(TGuid /*chunkReaderId*/) override
    { }

    void AddReadSessionInfo(TGuid /*readSessionId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TGuid GetId() const override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
