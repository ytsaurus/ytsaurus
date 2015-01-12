#pragma once

#include "public.h"

#include "data_statistics.h"
#include "multi_chunk_reader.h"

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/concurrency/nonblocking_queue.h>
#include <core/concurrency/public.h>

#include <core/logging/log.h>

#include <core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMultiChunkReaderBase
    : public virtual IMultiChunkReader
{
public:
    TMultiChunkReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NProto::TChunkSpec>& chunkSpecs);

    virtual TFuture<void> Open() override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual NProto::TDataStatistics GetDataStatistics() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

    virtual bool IsFetchingCompleted() const override;

protected:
    struct TSession
    {
        IChunkReaderBasePtr ChunkReader;
        int ChunkSpecIndex;

        void Reset()
        {
            ChunkReader = nullptr;
            ChunkSpecIndex = -1;
        }
    };

    NLog::TLogger Logger;

    TMultiChunkReaderConfigPtr Config_;
    TMultiChunkReaderOptionsPtr Options_;

    std::vector<NProto::TChunkSpec> ChunkSpecs_;

    TSession CurrentSession_;

    TFuture<void> ReadyEvent_;
    TPromise<void> CompletionError_;


    // ToDo(psushin): throw exceptions.
    virtual void DoOpen() = 0;

    virtual IChunkReaderBasePtr CreateTemplateReader(
        const NProto::TChunkSpec& chunkSpec,
        IChunkReaderPtr asyncReader) = 0;

    virtual void OnReaderOpened(IChunkReaderBasePtr chunkReader, int chunkIndex) = 0;

    virtual void OnReaderBlocked() = 0;

    virtual void OnReaderSwitched() = 0;

    virtual void OnReaderFinished();

    virtual void OnError();

    bool OnEmptyRead(bool readerFinished);

    void OpenPrefetchChunks();

    void RegisterFailedChunk(int chunkIndex);

private:
    IBlockCachePtr BlockCache_;
    NRpc::IChannelPtr MasterChannel_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    int PrefetchReaderIndex_;
    int PrefetchWindow_;

    NConcurrency::TParallelAwaiterPtr FetchingCompletedAwaiter_;

    TSpinLock FailedChunksLock_;
    std::vector<TChunkId> FailedChunks_;

    bool IsOpen_;

    int OpenedReaderCount_;

    TSpinLock DataStatisticsLock_;
    NProto::TDataStatistics DataStatistics_;
    yhash_set<IChunkReaderBasePtr> ActiveReaders_;

    // If KeepInMemory option is set, we store here references to finished readers.
    std::vector<IChunkReaderBasePtr> FinishedReaders_;


    IChunkReaderPtr CreateRemoteReader(const NProto::TChunkSpec& chunkSpec);

    void OpenNextChunk();
    void DoOpenNextChunk();

};

////////////////////////////////////////////////////////////////////////////////

class TSequentialMultiChunkReaderBase
    : public TMultiChunkReaderBase
{
public:
    TSequentialMultiChunkReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NProto::TChunkSpec>& chunkSpecs);

private:
    int NextReaderIndex_;
    std::vector<TPromise<IChunkReaderBasePtr>> NextReaders_;


    virtual void DoOpen() override;

    virtual void OnReaderOpened(IChunkReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override; 

    virtual void OnError() override;

    void WaitForNextReader();

    void WaitForCurrentReader();

};

////////////////////////////////////////////////////////////////////////////////

class TParallelMultiChunkReaderBase
    : public TMultiChunkReaderBase
{
public:
    TParallelMultiChunkReaderBase(
        TMultiChunkReaderConfigPtr config,
        TMultiChunkReaderOptionsPtr options,
        NRpc::IChannelPtr masterChannel,
        IBlockCachePtr blockCache,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
        const std::vector<NProto::TChunkSpec>& chunkSpecs);

private:
    typedef NConcurrency::TNonblockingQueue<TSession> TSessionQueue;

    TSessionQueue ReadySessions_;
    int FinishedReaderCount_;


    virtual void DoOpen() override;

    virtual void OnReaderOpened(IChunkReaderBasePtr chunkReader, int chunkIndex) override;

    virtual void OnReaderBlocked() override;

    virtual void OnReaderFinished() override;

    virtual void OnError() override;

    void WaitForReadyReader();

    void WaitForReader(TSession session);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
