#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/timing_statistics.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/chunk_client/data_statistics.h>
#include <yt/yt/client/chunk_client/reader_base.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/nonblocking_queue.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TMultiReaderManagerSession
{
    IReaderBasePtr Reader;
    int Index = -1;

    void Reset()
    {
        Reader.Reset();
        Index = -1;
    }
};

struct TMultiReaderManagerUnreadState
{
    IReaderBasePtr CurrentReader;
    std::vector<IReaderBasePtr> ActiveReaders;
    std::vector<IReaderFactoryPtr> ReaderFactories;
};

struct IMultiReaderManager
    : public virtual IReadyEventReaderBase
{
    virtual void Open() = 0;

    virtual NProto::TDataStatistics GetDataStatistics() const = 0;
    virtual TCodecStatistics GetDecompressionStatistics() const = 0;
    virtual NTableClient::TTimingStatistics GetTimingStatistics() const = 0;

    virtual bool IsFetchingCompleted() const = 0;

    virtual TMultiReaderManagerSession& GetCurrentSession() = 0;

    virtual const NLogging::TLogger& GetLogger() const = 0;

    virtual bool OnEmptyRead(bool readerFinished) = 0;

    virtual void RegisterFailedReader(const IReaderBasePtr& reader) = 0;

    virtual std::vector<TChunkId> GetFailedChunkIds() const = 0;

    virtual void Interrupt() = 0;

    virtual TMultiReaderManagerUnreadState GetUnreadState() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(), ReaderSwitched);
};

DEFINE_REFCOUNTED_TYPE(IMultiReaderManager)

////////////////////////////////////////////////////////////////////////////////

IMultiReaderManagerPtr CreateSequentialMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

IMultiReaderManagerPtr CreateParallelMultiReaderManager(
    TMultiChunkReaderConfigPtr config,
    TMultiChunkReaderOptionsPtr options,
    std::vector<IReaderFactoryPtr> readerFactories,
    IMultiReaderMemoryManagerPtr multiReaderMemoryManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
