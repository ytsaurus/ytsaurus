#pragma once

#include "public.h"

#include <ytlib/chunk_client/chunk_spec.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////\

class TNontemplateMultiChunkReaderBase
    : public virtual IMultiChunkReader
{
public:
    TNontemplateMultiChunkReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NProto::TChunkSpec>& chunkSpecs);

    virtual TAsyncError Open() override;

    virtual NProto::TDataStatistics GetDataStatistics() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

protected:
    NLog::TLogger& Logger;

    TMultiChunkReaderConfigPtr Config_;
    TMultiChunkReaderOptionsPtr Options_;

    std::vector<NProto::TChunkSpec> ChunkSpecs_;

    IBlockCachePtr BlockCache_;
    NRpc::IChannelPtr MasterChannel_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    int PrefetchWindow_;

    NConcurrency::TParallelAwaiterPtr FetchingCompleteAwaiter_;

    TSpinLock FailedChunksLock_;
    std::vector<TChunkId> FailedChunks_;

    TAsyncErrorPromise CompletionError_;
    TAsyncError ReadyEvent_;

    bool IsOpen_;

    int LastOpenedReaderIndex_;

    virtual TError DoOpen() = 0;

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateSequentialMultiChunkReaderBase
    : public TNontemplateMultiChunkReaderBase
{
public:

    virtual TAsyncError GetReadyEvent() override;

    virtual bool IsFetchingComplete() const override;

private:
    virtual TError DoOpen() override;
};

////////////////////////////////////////////////////////////////////////////////

/*
class TNontemplateParallelMultiChunkReaderBase
{
public:

private:
};
*/

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TReader>
class TMultiChunkReaderBase
{
    
};

////////////////////////////////////////////////////////////////////////////////

/*
bool Read(std::vector<TUnversionedRow>* rows)
{
    if (!HasMoreData())
        return false;

    auto readerFinished = !CurrentReader->Read(rows);
    if (!rows->empty()) {
        return true;
    }

    if (hasMore) {
        OnReaderBlocked();
        return true;
    } else {
        OnReaderEnded();
        return HasMoreData();
    }
}
*/

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
